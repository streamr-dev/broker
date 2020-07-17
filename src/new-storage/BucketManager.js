const debug = require('debug')('streamr:storage:bucket-manager')
const Heap = require('heap')
const { TimeUuid } = require('cassandra-driver').types

const Bucket = require('./Bucket')

const toKey = (streamId, partition) => `${streamId}-${partition}`

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
            debug(`stream ${key} found`)
            bucketId = this._findBucketId(key, timestamp)

            if (!bucketId) {
                const stream = this.streams[key]
                stream.minTimestamp = stream.minTimestamp !== undefined ? Math.min(stream.minTimestamp, timestamp) : timestamp
            }
        } else {
            debug(`stream ${key} not found, create new`)

            this.streams[key] = {
                streamId,
                partition,
                buckets: new Heap((a, b) => {
                    return b.dateCreate - a.dateCreate
                }),
                minTimestamp: timestamp
            }
        }

        return bucketId
    }

    incrementBucket(bucketId, size) {
        const bucket = this.buckets[bucketId]
        if (bucket) {
            bucket.incrementBucket(size, 1)
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
        debug(`checking stream: ${key}, timestamp: ${timestamp} in BucketManager state`)

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

        // just for debugging
        debug(`bucketId ${bucketId ? 'FOUND' : ' NOT FOUND'} for stream: ${key}, timestamp: ${timestamp}`)
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
            if (!latestBucket || (latestBucket && latestBucket.isAlmostFull())) {
                // eslint-disable-next-line no-await-in-loop
                const foundBuckets = await this.getLastBuckets(streamId, partition, 1)
                checkFoundBuckets(foundBuckets)
            }

            // check in database that we have bucket for minTimestamp
            if (!insertNewBucket && !this._findBucketId(key, minTimestamp)) {
                // eslint-disable-next-line no-await-in-loop
                const foundBuckets = await this.getLastBuckets(stream.streamId, stream.partition, 1, minTimestamp)
                checkFoundBuckets(foundBuckets)
            }

            if (insertNewBucket) {
                debug(`bucket for timestamp: ${minTimestamp} not found, create new bucket`)

                // we create first in memory, so don't wait for database, then _storeBuckets inserts bucket into database
                const newBucket = new Bucket(
                    TimeUuid.fromDate(new Date(minTimestamp)).toString(), streamId, partition, 0, 0, new Date(minTimestamp),
                    this.opts.maxBucketSize, this.opts.maxBucketRecords, this.opts.bucketKeepAliveSeconds
                )

                stream.buckets.push(newBucket)
                this.buckets[newBucket.getId()] = newBucket
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
        const GET_LAST_BUCKETS_RANGE_TIMESTAMP = 'SELECT * FROM bucket WHERE stream_id = ? and partition = ? AND date_create >= ? AND date_create <= ?'
        const GET_LAST_BUCKETS_FROM_TIMESTAMP = 'SELECT * FROM bucket WHERE stream_id = ? and partition = ? AND date_create >= ?'
        const GET_LAST_BUCKETS_TO_TIMESTAMP = 'SELECT * FROM bucket WHERE stream_id = ? and partition = ? AND date_create <= ?'

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
        const GET_LAST_BUCKETS = 'SELECT * FROM bucket WHERE stream_id = ? and partition = ? LIMIT ?'
        const GET_LAST_BUCKETS_TIMESTAMP = 'SELECT * FROM bucket WHERE stream_id = ? and partition = ? AND date_create <= ? LIMIT ?'

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
                console.error(e)
            }
        }

        return buckets
    }

    stop() {
        clearInterval(this._checkFullBucketsTimeout)
        clearInterval(this._storeBucketsTimeout)
    }

    _storeBuckets() {
        // for non-existing buckets UPDATE works as INSERT
        const UPDATE_BUCKET = 'UPDATE bucket SET size = ?, records = ?, id = ? WHERE stream_id = ? AND partition = ? AND date_create = ?'

        const notStoredBuckets = Object.values(this.buckets).filter((bucket) => !bucket.isStored())

        notStoredBuckets.forEach(async (bucket) => {
            const {
                id, size, records, streamId, partition, dateCreate
            } = bucket
            const params = [size, records, id, streamId, partition, dateCreate]

            try {
                // eslint-disable-next-line no-await-in-loop
                await this.cassandraClient.execute(UPDATE_BUCKET, params, {
                    prepare: true
                })

                if (bucket.records === records) {
                    bucket.setStored()
                }

                debug(`stored in database bucket state, params: ${params}`)
            } catch (e) {
                if (this.opts.logErrors) {
                    console.error(e)
                }
            }

            // if bucket is not in use and isStored, remove from memory
            if (!bucket.isAlive() && bucket.isStored()) {
                this._removeBucket(bucket.getId(), bucket.streamId, bucket.partition)
            }
        })

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
            stream.buckets = Heap.heapify(currentBuckets, (a, b) => {
                return b.dateCreate - a.dateCreate
            })
        }
    }
}

module.exports = BucketManager
