const Heap = require('heap')

const MAX_BUCKET_SIZE = 1024 * 1024 * 100
const MAX_BUCKET_RECORDS = 1000 * 1000

const toKey = (streamId, partition) => `${streamId}-${partition}`

class BucketManager {
    constructor(cassandraClient, logErrors = true) {
        this.streams = {}

        this.cassandraClient = cassandraClient
        this.logErrors = logErrors

        setInterval(() => {
            this._checkBuckets()
        }, 3000)
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
                    return a.dateCreate - b.dateCreate
                })),
                minTimestamp: timestamp,
                maxTimestamp: timestamp
            }
        }

        return bucketId
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

            if (!currentBuckets.length) {
                // eslint-disable-next-line no-await-in-loop
                const foundBuckets = await this.getLastBuckets(stream.streamId, stream.partition, 10)
                if (foundBuckets) {
                    foundBuckets.forEach((foundBucket) => {
                        stream.buckets.push(foundBucket)
                    })
                }
            }

            let latestBucketIsFull = false
            currentBuckets = stream.buckets.toArray()
            if (currentBuckets.length) {
                const firstRow = currentBuckets[0]
                if (firstRow.records >= MAX_BUCKET_RECORDS || firstRow.size >= MAX_BUCKET_SIZE) {
                    latestBucketIsFull = true
                }
            }

            if (!currentBuckets.length || latestBucketIsFull) {
                console.log('basket is fill or no buckets, insert new basket')
                // eslint-disable-next-line no-await-in-loop
                await this._insertNewBucket(stream.streamId, stream.partition)
            } else {
                console.log('current buckets')
                console.log(currentBuckets)
            }
        }
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
                    result.push({
                        id: row.id.toString(),
                        dateCreate: new Date(row.date_create).getTime(),
                        records: row.records,
                        size: row.size
                    })
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

    // stop() {
    //     clearInterval(this._checkBucketsInterval)
    // }
    //
    // addBucket(streamId, partition) {
    //     return this.getBucket(streamId, partition)
    // }
    //
    // getBucketId(streamId, partition) {
    //     const key = toKey(streamId, partition)
    //     const bucket = this.buckets.get(key)
    //     if (bucket) {
    //         return bucket.getUuid()
    //     }
    //     return undefined
    // }
    //
    // getBucket(streamId, partition) {
    //     const key = toKey(streamId, partition)
    //     let bucket = this.buckets.get(key)
    //     if (bucket) {
    //         bucket.updateTTL()
    //     } else {
    //         bucket = new Bucket(streamId, partition)
    //         this.buckets.set(key, bucket)
    //     }
    //     return bucket
    // }
    //
    // async resetBucket(bucket) {
    //     await this._storeBucket(bucket)
    //     bucket.reset()
    // }
    //
    // async incrementBucket(streamId, partition, records, size) {
    //     const bucket = this.getBucket(streamId, partition)
    //     bucket.incrementBucket(records, size)
    //     if (bucket.getUuid()) {
    //         await this._storeBucket(bucket)
    //     }
    // }
    //
    // async _storeBucket(bucket) {
    //     const {
    //         records, size, uuid, streamId, partition, dateCreate
    //     } = bucket
    //
    //     const UPDATE_BUCKET = 'UPDATE bucket SET records = ?, size = ? WHERE stream_id = ? AND partition = ? AND date_create = ?'
    //     const params = [records, size, streamId, partition, new Date(dateCreate).getTime()]
    //     console.log(UPDATE_BUCKET)
    //     console.log(params)
    //     return this.cassandraClient.execute(UPDATE_BUCKET, params, { prepare: true })
    // }
    //
    // async _getLastNotFullBucketOrCreate(streamId, partition) {
    //     let bucket
    //
    //     const INSERT_NEW_BUCKET = 'INSERT INTO bucket (id, stream_id, partition, date_create, records, size) '
    //                                + 'VALUES (now(), ?, ?, toTimestamp(now()), 0, 0)'
    //     const GET_LAST_BUCKET = 'SELECT * FROM bucket WHERE stream_id = ? AND partition = ? LIMIT 1'
    //
    //     const params = [streamId, partition]
    //     const resultSet = await this.cassandraClient.execute(GET_LAST_BUCKET, params, {
    //         prepare: true,
    //     })
    //
    //     let bucketIsFull = false
    //     if (resultSet.rows.length) {
    //         const row = resultSet.rows[0]
    //         if (row.records < MAX_BUCKET_RECORDS && row.size < MAX_BUCKET_SIZE) {
    //             console.log('found bucket')
    //             console.log(row)
    //             bucket = row
    //         } else {
    //             bucketIsFull = true
    //         }
    //     }
    //
    //     if (!resultSet.rows.length || bucketIsFull) {
    //         console.log('basket is fill, insert new basket')
    //         await this.cassandraClient.execute(INSERT_NEW_BUCKET, params, {
    //             prepare: true,
    //         })
    //     }
    //
    //     return bucket
    // }
    //
    // async _checkBuckets() {
    //     console.log('checking buckets')
    //     const buckets = [...this.buckets.values()]
    //
    //     for (const bucket of buckets) {
    //         console.log(bucket)
    //         const { streamId, partition, records, size } = bucket
    //
    //         if (!bucket.getUuid()) {
    //             console.log('empty bucket')
    //             // eslint-disable-next-line no-await-in-loop
    //             const found = await this._getLastNotFullBucketOrCreate(streamId, partition)
    //             if (found) {
    //                 bucket.updateUuid(found.id.toString())
    //                 bucket.dateCreate = found.date_create
    //                 bucket.size = found.size
    //                 bucket.records = found.records
    //             }
    //         }
    //
    //         if (bucket.getUuid() && (records >= this.maxBucketRecords || size > this.maxBucketSize)) {
    //             console.log(`full backet: records - ${records}, maxBucketRecords: ${this.maxBucketRecords}, size: ${size}, maxSize: ${this.maxBucketSize}`)
    //             this.resetBucket(bucket)
    //         }
    //     }
    // }
}

module.exports = BucketManager
