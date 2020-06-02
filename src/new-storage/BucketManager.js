const Heap = require('heap')

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
        this._storeBucketsInterval = setInterval(() => this._storeBuckets(), 10000)
    }

    getBucketId(streamId, partition, timestamp) {
        let bucketId
        const key = toKey(streamId, partition)

        if (this.streams[key]) {
            const stream = this.streams[key]

            this.streams[key] = {
                streamId: stream.streamId,
                partition: stream.partition,
                buckets: stream.buckets,
                minTimestamp: Math.min(stream.minTimestamp, timestamp),
                maxTimestamp: Math.min(stream.maxTimestamp, timestamp),
            }

            bucketId = this._findBucketId(key, timestamp)
        } else {
            this.streams[key] = {
                streamId,
                partition,
                buckets: new Heap(((a, b) => {
                    return b.dateCreate - a.dateCreate
                })),
                minTimestamp: timestamp,
                maxTimestamp: timestamp
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
            const stream = this.streams[streamId]
            let currentBuckets = stream.buckets.toArray()

            // eslint-disable-next-line no-await-in-loop
            const foundBuckets = await this.getLastBuckets(stream.streamId, stream.partition, 10)
            if (foundBuckets) {
                foundBuckets.forEach((foundBucket) => {
                    if (!(foundBucket.id in this.buckets)) {
                        stream.buckets.push(foundBucket)
                        this.buckets[foundBucket.id] = foundBucket
                    }
                })
            }

            let latestBucketIsFull = false
            currentBuckets = stream.buckets.toArray()
            if (currentBuckets.length) {
                const firstBucketId = currentBuckets[0].id
                if (this.buckets[firstBucketId].isFull()) {
                    latestBucketIsFull = true
                }
            }

            if (!currentBuckets.length || latestBucketIsFull) {
                console.log('basket is fill or no buckets, insert new basket')
                // eslint-disable-next-line no-await-in-loop
                await this._insertNewBucket(stream.streamId, stream.partition)
            } else {
                console.log('current buckets')
            }
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
        const GET_LAST_BUCKETS_TIMESTAMP = 'SELECT * FROM bucket WHERE stream_id = ? and partition = ? AND date_create < ? LIMIT ?'

        const result = []

        const params = [streamId, partition, limit]
        if (timestamp) {
            params.push(timestamp)
        }

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

    async _insertNewBucket(streamId, partition) {
        const INSERT_NEW_BUCKET = 'INSERT INTO bucket (stream_id, partition, date_create, records, size, id) '
                                    + 'VALUES (?, ?, toTimestamp(now()), 0, 0, now())'
        const params = [streamId, partition]

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

            // if expired removed
        }
    }
}

module.exports = BucketManager
