const Heap = require('heap')
const { TimeUuid } = require('cassandra-driver').types

const Bucket = require('./Bucket')

const MAX_BUCKET_SIZE = 100000 // 1024 * 1024 * 100
const MAX_BUCKET_RECORDS = 10 // 1000 * 1000

const toKey = (streamId, partition) => `${streamId}-${partition}`

class BucketManager {
    constructor(cassandraClient, logErrors = true) {
        this.streams = {}
        this.buckets = {}

        this.cassandraClient = cassandraClient
        this.logErrors = logErrors

        this._checkBucketsInterval = setInterval(() => this._checkBuckets(), 3000)
        this._storeBucketsInterval = setInterval(() => this._storeBuckets(), 3000)
    }

    getBucketId(streamId, partition, timestamp) {
        let bucketId

        const key = toKey(streamId, partition)
        if (this.streams[key]) {
            bucketId = this._findBucketId(key, timestamp)

            if (!bucketId) {
                const stream = this.streams[key]
                const { max, min } = stream.timestamps

                stream.timestamps.max = Math.max(max, timestamp)
                stream.timestamps.min = Math.min(min, timestamp)
                stream.timestamps.avg = Math.ceil((stream.timestamps.max + stream.timestamps.min) / 2)

                this.streams[key] = stream
            }
        } else {
            this.streams[key] = {
                streamId,
                partition,
                buckets: new Heap(((a, b) => {
                    return b.dateCreate - a.dateCreate
                })),
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
        console.log(`looking for stream: ${key}, timestamp: ${timestamp}`)

        const stream = this.streams[key]
        if (stream) {
            const currentBuckets = stream.buckets.toArray()
            console.log(currentBuckets)
            for (let i = 0; i < currentBuckets.length; i++) {
                if (currentBuckets[i].dateCreate < timestamp) {
                    bucketId = currentBuckets[i].id
                    break
                }
            }
        }

        return bucketId
    }

    async _checkBuckets() {
        // eslint-disable-next-line no-restricted-syntax
        for (const streamId of Object.keys(this.streams)) {
            let insertNewBucket = false

            const stream = this.streams[streamId]
            const minTimestamp = stream.timestamps.min

            const bucketId = this._findBucketId(streamId, minTimestamp)
            if (!bucketId) {
                // eslint-disable-next-line no-await-in-loop
                const foundBuckets = await this.getLastBuckets(stream.streamId, stream.partition, 1, minTimestamp)
                const foundBucket = foundBuckets.length ? foundBuckets[0] : undefined

                if (foundBucket && !(foundBucket.id in this.buckets)) {
                    stream.buckets.push(foundBucket)
                    this.buckets[foundBucket.id] = foundBucket
                    stream.timestamps.min = undefined
                } else {
                    insertNewBucket = true
                }
            }

            // check latest known
            const currentBuckets = stream.buckets.toArray()
            if (currentBuckets.length) {
                const latestBucket = currentBuckets[0]
                insertNewBucket = latestBucket.isFull()
                if (latestBucket.isFull()) {
                    console.log(`latest bucket isFull: ${latestBucket.isFull()}`)
                    insertNewBucket = true

                    // eslint-disable-next-line no-await-in-loop
                    const foundBuckets = await this.getLastBuckets(stream.streamId, stream.partition, 1)
                    const foundBucket = foundBuckets.length ? foundBuckets[0] : undefined

                    // waiting for new bucket
                    if (foundBucket && !foundBucket.isFull() && !(foundBucket.id in this.buckets)) {
                        stream.buckets.push(foundBucket)
                        this.buckets[foundBucket.id] = foundBucket
                        stream.timestamps.min = undefined
                    }
                }
            }

            if (insertNewBucket) {
                console.log(`bucket for timestamp: ${minTimestamp} not found, insert new basket`)
                // eslint-disable-next-line no-await-in-loop
                await this._insertNewBucket(stream.streamId, stream.partition, minTimestamp)
            }

            this.streams[streamId] = stream
        }
    }

    async getBucketsFromTimestamp(streamId, partition, fromTimestamp) {
        const GET_LAST_BUCKETS_TIMESTAMP = 'SELECT * FROM bucket WHERE stream_id = ? and partition = ? AND date_create >= ?'
        const params = [streamId, partition, fromTimestamp]

        const buckets = []

        try {
            console.log(GET_LAST_BUCKETS_TIMESTAMP)
            console.log(params, fromTimestamp)

            const resultSet = await this.cassandraClient.execute(GET_LAST_BUCKETS_TIMESTAMP, params, {
                prepare: true,
            })

            if (resultSet.rows.length) {
                resultSet.rows.forEach((row) => {
                    const { id, records, size, date_create: dateCreate } = row

                    const bucket = new Bucket(
                        id.toString(), streamId, partition, size, records,
                        new Date(dateCreate).getTime(), MAX_BUCKET_RECORDS, MAX_BUCKET_SIZE
                    )

                    buckets.push(bucket)
                })
            }
        } catch (e) {
            console.error(e)
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
                        id.toString(), streamId, partition, size, records,
                        new Date(dateCreate).getTime(), MAX_BUCKET_RECORDS, MAX_BUCKET_SIZE
                    )

                    result.push(bucket)
                })
            }
        } catch (e) {
            console.error(e)
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
        } catch (e) {
            console.error(e)
        }
    }

    stop() {
        clearInterval(this._checkBucketsInterval)
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

            // eslint-disable-next-line no-await-in-loop
            await this.cassandraClient.execute(UPDATE_BUCKET, params, {
                prepare: true
            })
        }
    }
}

module.exports = BucketManager
