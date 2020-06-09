const debug = require('debug')('streamr:storage:bucket-manager')
const Heap = require('heap')
const { TimeUuid } = require('cassandra-driver').types

const Bucket = require('./Bucket')

const toKey = (streamId, partition) => `${streamId}-${partition}`

class BucketManager {
    constructor(cassandraClient, opts) {
        const defaultOptions = {
            logErrors: true,
            checkFullBucketsTimeout: 1000,
            storeBucketsTimeout: 1000,
            maxBucketSize: 10000,
            maxBucketRecords: 10,
            bucketKeepAliveSeconds: 60
        }

        this.opts = {
            ...defaultOptions, ...opts
        }

        this.streams = {}
        this.buckets = {}

        this.cassandraClient = cassandraClient

        this._checkFullBuckets()
        this._storeBuckets()
    }

    getBucketId(streamId, partition, timestamp) {
        let bucketId

        const key = toKey(streamId, partition)
        if (this.streams[key]) {
            debug(`stream ${key} found`)
            bucketId = this._findBucketId(key, timestamp)

            const stream = this.streams[key]
            const { max, min } = stream.timestamps

            stream.timestamps.max = max ? Math.max(max, timestamp) : timestamp
            stream.timestamps.min = min ? Math.min(min, timestamp) : timestamp
            stream.timestamps.avg = Math.ceil((stream.timestamps.max + stream.timestamps.min) / 2)

            this.streams[key] = stream
        } else {
            debug(`stream ${key} not found, create new`)

            this.streams[key] = {
                streamId,
                partition,
                buckets: new Heap((a, b) => {
                    return b.dateCreate - a.dateCreate
                }),
                timestamps: {
                    max: timestamp,
                    avg: timestamp,
                    min: timestamp
                }
            }
        }

        return bucketId
    }

    incrementBucket(bucketId, size, records = 1) {
        const bucket = this.buckets[bucketId]
        if (bucket) {
            bucket.incrementBucket(size, records)
        }
    }

    _findBucketId(key, timestamp) {
        let bucketId
        debug(`checking stream: ${key}, timestamp: ${timestamp} in BucketManager state`)

        const stream = this.streams[key]
        if (stream) {
            const currentBuckets = stream.buckets.toArray()
            debug('======> checking buckets')
            for (let i = 0; i < currentBuckets.length; i++) {
                debug(`bucketId: ${currentBuckets[i].getId()}, ${currentBuckets[i].dateCreate} <= ${new Date(timestamp)} = ${currentBuckets[i].dateCreate <= new Date(timestamp)}`)
                if (currentBuckets[i].dateCreate <= new Date(timestamp)) {
                    bucketId = currentBuckets[i].getId()
                    debug(`bucketId ${bucketId} FOUND for stream: ${key}, timestamp: ${timestamp}`)
                    break
                }
            }
        }

        if (!bucketId) {
            debug(`bucketId NOT FOUND for stream: ${key}, timestamp: ${timestamp} `)
        }

        return bucketId
    }

    async _checkFullBuckets() {
        const streamIds = Object.keys(this.streams)

        for (let i = 0; i < streamIds.length; i++) {
            const stream = this.streams[streamIds[i]]
            const minTimestamp = stream.timestamps.min

            console.log(stream)

            if (!minTimestamp) {
                console.log('=====>skiiping')
                // eslint-disable-next-line no-continue
                continue
            }

            console.log('=====>checkkiii')

            // check latest known
            const currentBuckets = stream.buckets.toArray()
            let isFullOrNotFound = currentBuckets.length === 0
            let latestBucketId
            if (currentBuckets.length) {
                const latestBucket = currentBuckets[0]

                if (latestBucket.isFull()) {
                    latestBucketId = latestBucket.getId()
                    debug(`latest bucket ${latestBucketId} isFull: ${latestBucket.isFull()}`)
                    isFullOrNotFound = true
                } else {
                    stream.timestamps.min = undefined
                }
            }

            // try to get latest from database
            if (isFullOrNotFound) {
                // eslint-disable-next-line no-await-in-loop
                const foundBuckets = await this.getLastBuckets(stream.streamId, stream.partition, 1)
                const foundBucket = foundBuckets.length ? foundBuckets[0] : undefined

                // add if found new not empty bucket
                if (foundBucket && !foundBucket.isFull() && latestBucketId !== foundBucket.getId() && !(foundBucket.getId() in this.buckets)) {
                    stream.buckets.push(foundBucket)
                    this.buckets[foundBucket.getId()] = foundBucket

                    stream.timestamps.min = undefined
                    isFullOrNotFound = false
                }
            }

            // not found in state or in database, add new
            if (isFullOrNotFound) {
                debug(`bucket for timestamp: ${minTimestamp} not found, insert new basket`)
                // eslint-disable-next-line no-await-in-loop
                await this._insertNewBucket(stream.streamId, stream.partition, minTimestamp)
            }

            this.streams[streamIds[i]] = stream
        }

        setTimeout(() => this._checkFullBuckets(), this.opts.checkFullBucketsTimeout)
    }

    async getBucketsByTimestamp(streamId, partition, fromTimestamp = undefined, toTimestamp = undefined) {
        const GET_LAST_BUCKETS_RANGE_TIMESTAMP = 'SELECT * FROM bucket WHERE stream_id = ? and partition = ? AND date_create >= ? AND date_create <= ?'
        const GET_LAST_BUCKETS_FROM_TIMESTAMP = 'SELECT * FROM bucket WHERE stream_id = ? and partition = ? AND date_create >= ?'
        const GET_LAST_BUCKETS_TO_TIMESTAMP = 'SELECT * FROM bucket WHERE stream_id = ? and partition = ? AND date_create <= ?'

        let query
        let params

        if (fromTimestamp && toTimestamp) {
            query = GET_LAST_BUCKETS_RANGE_TIMESTAMP
            params = [streamId, partition, fromTimestamp, toTimestamp]
        } else if (fromTimestamp && !toTimestamp) {
            query = GET_LAST_BUCKETS_FROM_TIMESTAMP
            params = [streamId, partition, fromTimestamp]
        } else if (!fromTimestamp && toTimestamp) {
            query = GET_LAST_BUCKETS_TO_TIMESTAMP
            params = [streamId, partition, toTimestamp]
        } else {
            throw TypeError('Not correct combination of fromTimestamp and toTimestamp')
        }

        const buckets = []

        try {
            const resultSet = await this.cassandraClient.execute(query, params, {
                prepare: true,
            })

            if (resultSet.rows.length) {
                resultSet.rows.forEach((row) => {
                    const { id, records, size, date_create: dateCreate } = row

                    const bucket = new Bucket(
                        id.toString(), streamId, partition, size, records, new Date(dateCreate),
                        this.opts.maxBucketSize, this.opts.maxBucketRecords, this.opts.bucketKeepAliveSeconds
                    )

                    buckets.push(bucket)
                })
            }
        } catch (e) {
            if (this.opts.logErrors) {
                console.error(e)
            }
        }

        return buckets
    }

    async getLastBuckets(streamId, partition, limit = 1, timestamp = undefined) {
        const GET_LAST_BUCKETS = 'SELECT * FROM bucket WHERE stream_id = ? and partition = ? LIMIT ?'
        const GET_LAST_BUCKETS_TIMESTAMP = 'SELECT * FROM bucket WHERE stream_id = ? and partition = ? AND date_create <= ? LIMIT ?'

        const result = []
        const params = timestamp ? [streamId, partition, timestamp, limit] : [streamId, partition, limit]

        try {
            const resultSet = await this.cassandraClient.execute(timestamp ? GET_LAST_BUCKETS_TIMESTAMP : GET_LAST_BUCKETS, params, {
                prepare: true,
            })

            if (resultSet.rows.length) {
                resultSet.rows.forEach((row) => {
                    const { id, records, size, date_create: dateCreate } = row

                    const bucket = new Bucket(
                        id.toString(), streamId, partition, size, records, new Date(dateCreate),
                        this.opts.maxBucketSize, this.opts.maxBucketRecords, this.opts.bucketKeepAliveSeconds
                    )

                    debug(`found bucket: ${bucket.getId()}, size: ${size}, records: ${records}, dateCreate: ${bucket.dateCreate}`)
                    debug(`for streamId: ${streamId}, partition: ${partition} ${timestamp ? `,timestamp: ${timestamp}` : ''}, limit: ${limit}`)

                    result.push(bucket)
                })
            } else {
                debug(`getLastBuckets: no buckets found for streamId: ${streamId} partition: ${partition} ${timestamp ? `,timestamp: ${timestamp}` : ''}, limit: ${limit}`)
            }
        } catch (e) {
            if (this.opts.logErrors) {
                console.error(e)
            }
        }

        return result
    }

    async _insertNewBucket(streamId, partition, timestamp = new Date()) {
        const INSERT_NEW_BUCKET = 'INSERT INTO bucket (stream_id, partition, date_create, records, size, id) '
                                    + 'VALUES (?, ?, ?, 0, 0, ?)'
        const params = [streamId, partition, timestamp, TimeUuid.fromDate(new Date(timestamp))]

        try {
            await this.cassandraClient.execute(INSERT_NEW_BUCKET, params, {
                prepare: true,
            })
            if (Number.isNaN(timestamp)) {
                console.log('isNaN timestamp')
            }
            debug(`inserted new bucket for streamId: ${streamId}, partition: ${partition}, timestamp: ${timestamp}`)
        } catch (e) {
            if (this.opts.logErrors) {
                console.error(e)
            }
        }
    }

    stop() {
        clearInterval(this._checkFullBucketsInterval)
        clearInterval(this._storeBucketsInterval)
    }

    async _storeBuckets() {
        const UPDATE_BUCKET = 'UPDATE bucket SET size = ?, records = ? WHERE stream_id = ? AND partition = ? AND date_create = ?'

        // eslint-disable-next-line no-restricted-syntax
        for (const bucket of Object.values(this.buckets)) {
            const {
                size, records, streamId, partition, dateCreate
            } = bucket
            const params = [size, records, streamId, partition, dateCreate]

            try {
                // eslint-disable-next-line no-await-in-loop
                await this.cassandraClient.execute(UPDATE_BUCKET, params, {
                    prepare: true
                })
                debug(`stored in database bucket state, params: ${params}`)
            } catch (e) {
                if (this.opts.logErrors) {
                    console.error(e)
                }
            }

            if (!bucket.isAlive()) {
                this._removeBucket(bucket.getId(), bucket.streamId, bucket.partition)
            }
        }

        setTimeout(() => this._storeBuckets(), this.opts.storeBucketsTimeout)
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
            stream.buckets = Heap.heapify(currentBuckets, (a, b) => {
                return b.dateCreate - a.dateCreate
            })
            this.streams[key] = stream
        }
    }
}

module.exports = BucketManager
