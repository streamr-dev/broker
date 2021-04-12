const Heap = require('heap')
const { TimeUuid } = require('cassandra-driver').types
const allSettled = require('promise.allsettled')

const logger = require('../helpers/logger')('streamr:storage:BucketManager')

const Bucket = require('./Bucket')

const toKey = (streamId, partition) => `${streamId}-${partition}`

const instantiateNewHeap = () => new Heap((a, b) => {
    return b.dateCreate - a.dateCreate
})

class BucketManager {
    constructor(cassandraClient, opts = {}) {
        const defaultOptions = {
            logErrors: true,
            checkFullBucketsTimeout: 1000,
            storeBucketsTimeout: 500,
            maxBucketSize: 1024 * 1024 * 100,
            maxBucketRecords: 500 * 1000,
            bucketKeepAliveSeconds: 60
        }

        this.opts = {
            ...defaultOptions,
            ...opts
        }

        this.streams = {}
        this.buckets = {}

        this.cassandraClient = cassandraClient

        this._checkFullBucketsTimeout = undefined
        this._storeBucketsTimeout = undefined

        this._checkFullBuckets()
        this._storeBuckets()
    }

    getBucketId(streamId, partition, timestamp) {
        let bucketId

        const key = toKey(streamId, partition)

        if (this.streams[key]) {
            logger.debug(`stream ${key} found`)
            bucketId = this._findBucketId(key, timestamp)

            if (!bucketId) {
                const stream = this.streams[key]
                stream.minTimestamp = stream.minTimestamp !== undefined ? Math.min(stream.minTimestamp, timestamp) : timestamp
            }
        } else {
            logger.debug(`stream ${key} not found, create new`)

            this.streams[key] = {
                streamId,
                partition,
                buckets: instantiateNewHeap(),
                minTimestamp: timestamp
            }
        }

        return bucketId
    }

    incrementBucket(bucketId, size) {
        const bucket = this.buckets[bucketId]
        if (bucket) {
            bucket.incrementBucket(size)
        } else {
            logger.warn(`${bucketId} not found`)
        }
    }

    _getLatestInMemoryBucket(key) {
        const stream = this.streams[key]
        if (stream) {
            return stream.buckets.peek()
        }
        return undefined
    }

    _findBucketId(key, timestamp) {
        let bucketId
        logger.debug(`checking stream: ${key}, timestamp: ${timestamp} in BucketManager state`)

        const stream = this.streams[key]
        if (stream) {
            const latestBucket = this._getLatestInMemoryBucket(key)

            if (latestBucket) {
                // latest bucket is younger than timestamp
                if (!latestBucket.isAlmostFull() && latestBucket.dateCreate <= new Date(timestamp)) {
                    bucketId = latestBucket.getId()
                    // timestamp is in the past
                } else if (latestBucket.dateCreate > new Date(timestamp)) {
                    const currentBuckets = stream.buckets.toArray()
                    // remove latest
                    currentBuckets.shift()

                    for (let i = 0; i < currentBuckets.length; i++) {
                        if (currentBuckets[i].dateCreate <= new Date(timestamp)) {
                            bucketId = currentBuckets[i].getId()
                            break
                        }
                    }
                }
            }
        }

        // just for logger.debugging
        logger.debug(`bucketId ${bucketId ? 'FOUND' : ' NOT FOUND'} for stream: ${key}, timestamp: ${timestamp}`)
        return bucketId
    }

    async _checkFullBuckets() {
        const streamIds = Object.keys(this.streams)

        for (let i = 0; i < streamIds.length; i++) {
            const stream = this.streams[streamIds[i]]
            const { streamId, partition } = stream
            const { minTimestamp } = stream

            // minTimestamp is undefined if all buckets are found
            if (minTimestamp === undefined) {
                // eslint-disable-next-line no-continue
                continue
            }

            let insertNewBucket = false

            // helper function
            const checkFoundBuckets = (foundBuckets) => {
                const foundBucket = foundBuckets.length ? foundBuckets[0] : undefined

                if (foundBucket && !(foundBucket.getId() in this.buckets)) {
                    stream.buckets.push(foundBucket)
                    this.buckets[foundBucket.getId()] = foundBucket
                    stream.minTimestamp = undefined
                } else {
                    insertNewBucket = true
                }
            }

            // check in memory
            const key = toKey(streamId, partition)
            const latestBucket = this._getLatestInMemoryBucket(key)
            if (latestBucket) {
                // if latest is full or almost full - create new bucket
                insertNewBucket = latestBucket.isAlmostFull()
            }

            // if latest is not found or it's full => try to find latest in database
            if (!latestBucket || latestBucket.isAlmostFull()) {
                // eslint-disable-next-line no-await-in-loop
                const foundBuckets = await this.getLastBuckets(streamId, partition, 1)
                checkFoundBuckets(foundBuckets)
            }

            // check in database that we have bucket for minTimestamp
            if (!insertNewBucket && !this._findBucketId(key, minTimestamp)) {
                // eslint-disable-next-line no-await-in-loop
                const foundBuckets = await this.getLastBuckets(streamId, partition, 1, minTimestamp)
                checkFoundBuckets(foundBuckets)
            }

            if (insertNewBucket) {
                logger.debug(`bucket for timestamp: ${minTimestamp} not found, create new bucket`)

                // we create first in memory, so don't wait for database, then _storeBuckets inserts bucket into database
                const newBucket = new Bucket(
                    TimeUuid.fromDate(new Date(minTimestamp)).toString(), streamId, partition, 0, 0, new Date(minTimestamp),
                    this.opts.maxBucketSize, this.opts.maxBucketRecords, this.opts.bucketKeepAliveSeconds
                )

                stream.buckets.push(newBucket)
                this.buckets[newBucket.getId()] = newBucket
                // eslint-disable-next-line require-atomic-updates
                stream.minTimestamp = undefined
            }
        }

        this._checkFullBucketsTimeout = setTimeout(() => this._checkFullBuckets(), this.opts.checkFullBucketsTimeout)
    }

    /**
     * Get buckets by timestamp range or all known from some timestamp or all buckets before some timestamp
     *
     * @param streamId
     * @param partition
     * @param fromTimestamp
     * @param toTimestamp
     * @returns {Promise<[]>}
     */
    async getBucketsByTimestamp(streamId, partition, fromTimestamp = undefined, toTimestamp = undefined) {
        const getExplicitFirst = () => {
            // if fromTimestamp is defined, the first data point are in a some earlier bucket
            // (bucket.dateCreated<=fromTimestamp as data within one millisecond won't be divided to multiple buckets)
            const QUERY = 'SELECT * FROM bucket WHERE stream_id = ? and partition = ? AND date_create <= ? ORDER BY date_create DESC LIMIT 1'
            const params = [streamId, partition, fromTimestamp]
            return this._getBucketsFromDatabase(QUERY, params, streamId, partition)
        }

        const getRest = () => {
            const GET_LAST_BUCKETS_RANGE_TIMESTAMP = 'SELECT * FROM bucket WHERE stream_id = ? and partition = ? AND date_create > ? AND date_create <= ? ORDER BY date_create DESC'
            const GET_LAST_BUCKETS_FROM_TIMESTAMP = 'SELECT * FROM bucket WHERE stream_id = ? and partition = ? AND date_create > ? ORDER BY date_create DESC'
            const GET_LAST_BUCKETS_TO_TIMESTAMP = 'SELECT * FROM bucket WHERE stream_id = ? and partition = ? AND date_create <= ? ORDER BY date_create DESC'
            let query
            let params
            if (fromTimestamp !== undefined && toTimestamp !== undefined) {
                query = GET_LAST_BUCKETS_RANGE_TIMESTAMP
                params = [streamId, partition, fromTimestamp, toTimestamp]
            } else if (fromTimestamp !== undefined && toTimestamp === undefined) {
                query = GET_LAST_BUCKETS_FROM_TIMESTAMP
                params = [streamId, partition, fromTimestamp]
            } else if (fromTimestamp === undefined && toTimestamp !== undefined) {
                query = GET_LAST_BUCKETS_TO_TIMESTAMP
                params = [streamId, partition, toTimestamp]
            } else {
                throw TypeError(`Not correct combination of fromTimestamp (${fromTimestamp}) and toTimestamp (${toTimestamp})`)
            }
            return this._getBucketsFromDatabase(query, params, streamId, partition)
        }

        if (fromTimestamp !== undefined) {
            return Promise.all([getExplicitFirst(), getRest()])
                .then(([first, rest]) => rest.concat(first))
        } else { // eslint-disable-line no-else-return
            return getRest()
        }
    }

    /**
     * Get latest N buckets or get latest N buckets before some date (to check buckets in the past)
     *
     * @param streamId
     * @param partition
     * @param limit
     * @param timestamp
     * @returns {Promise<[]>}
     */
    async getLastBuckets(streamId, partition, limit = 1, timestamp = undefined) {
        const GET_LAST_BUCKETS = 'SELECT * FROM bucket WHERE stream_id = ? and partition = ?  ORDER BY date_create DESC LIMIT ?'
        const GET_LAST_BUCKETS_TIMESTAMP = 'SELECT * FROM bucket WHERE stream_id = ? and partition = ? AND date_create <= ? ORDER BY date_create DESC LIMIT ?'

        let query
        let params

        if (timestamp) {
            query = GET_LAST_BUCKETS_TIMESTAMP
            params = [streamId, partition, timestamp, limit]
        } else {
            query = GET_LAST_BUCKETS
            params = [streamId, partition, limit]
        }

        return this._getBucketsFromDatabase(query, params, streamId, partition)
    }

    async getFirstBucket(streamId, partition) {
        const QUERY = 'SELECT * FROM bucket WHERE stream_id = ? and partition = ? ORDER BY date_create ASC LIMIT ?'
        const params = [streamId, partition, 1]
        return this._getBucketsFromDatabase(QUERY, params, streamId, partition)
    }

    async _getBucketsFromDatabase(query, params, streamId, partition) {
        const buckets = []

        try {
            const resultSet = await this.cassandraClient.execute(query, params, {
                prepare: true,
            })

            resultSet.rows.forEach((row) => {
                const { id, records, size, date_create: dateCreate } = row

                const bucket = new Bucket(
                    id.toString(), streamId, partition, size, records, new Date(dateCreate),
                    this.opts.maxBucketSize, this.opts.maxBucketRecords, this.opts.bucketKeepAliveSeconds
                )

                buckets.push(bucket)
            })
        } catch (e) {
            if (this.opts.logErrors) {
                logger.error(e)
            }
        }

        return buckets
    }

    stop() {
        clearInterval(this._checkFullBucketsTimeout)
        clearInterval(this._storeBucketsTimeout)
    }

    async _storeBuckets() {
        // for non-existing buckets UPDATE works as INSERT
        const UPDATE_BUCKET = 'UPDATE bucket SET size = ?, records = ?, id = ? WHERE stream_id = ? AND partition = ? AND date_create = ?'

        const notStoredBuckets = Object.values(this.buckets).filter((bucket) => !bucket.isStored())

        const results = await allSettled(notStoredBuckets.map(async (bucket) => {
            const {
                id, size, records, streamId, partition, dateCreate
            } = bucket
            const params = [size, records, id, streamId, partition, dateCreate]

            await this.cassandraClient.execute(UPDATE_BUCKET, params, {
                prepare: true
            })
            return {
                bucket,
                records
            }
        }))

        results.forEach((result) => {
            if (result.status === 'fulfilled') {
                const { bucket: storedBucket, records } = result.value

                if (storedBucket.records === records) {
                    storedBucket.setStored()
                }

                if (!storedBucket.isAlive() && storedBucket.isStored()) {
                    this._removeBucket(storedBucket.getId(), storedBucket.streamId, storedBucket.partition)
                }
            }
        })

        const bucketsToRemove = Object.values(this.buckets).filter((bucket) => bucket.isStored() && !bucket.isAlive())
        bucketsToRemove.forEach((bucket) => this._removeBucket(bucket.getId(), bucket.streamId, bucket.partition))

        this._storeBucketsTimeout = setTimeout(() => this._storeBuckets(), this.opts.storeBucketsTimeout)
    }

    _removeBucket(bucketId, streamId, partition) {
        delete this.buckets[bucketId]

        const key = toKey(streamId, partition)
        const stream = this.streams[key]
        if (stream) {
            const currentBuckets = stream.buckets.toArray()
            for (let i = 0; i < currentBuckets.length; i++) {
                if (currentBuckets[i].getId() === bucketId) {
                    delete currentBuckets[i]
                    break
                }
            }

            // after removing we need to rebuild heap
            stream.buckets = instantiateNewHeap()
            currentBuckets.forEach((bucket) => {
                stream.buckets.push(bucket)
            })
        }
    }
}

module.exports = BucketManager
