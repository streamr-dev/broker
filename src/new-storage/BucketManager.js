const MAX_BUCKET_SIZE = 10000
const MAX_BUCKET_RECORDS = 100
const CHECK_BUCKETS_INTERVAL = 1000

const toKey = (streamId, partition) => `${streamId}-${partition}`

class BucketManager {
    constructor(cassandraClient, maxBucketSize = MAX_BUCKET_SIZE, maxBucketRecords = MAX_BUCKET_RECORDS, checkBucketsInterval = CHECK_BUCKETS_INTERVAL) {
        this.cassandraClient = cassandraClient
        this.maxBucketSize = maxBucketSize
        this.maxBucketRecords = maxBucketRecords
        this.buckets = new Map()

        this._checkBucketsInterval = setInterval(() => {
            this._checkBuckets()
        }, checkBucketsInterval)
    }

    stop() {
        clearInterval(this._checkBucketsInterval)
    }

    addBucket(streamId, partition) {
        return this.getBucket(streamId, partition)
    }

    getBucketId(streamId, partition) {
        const key = toKey(streamId, partition)
        const bucket = this.buckets.get(key)
        if (bucket) {
            return bucket.getUuid()
        }
        return undefined
    }

    getBucket(streamId, partition) {
        const key = toKey(streamId, partition)
        let bucket = this.buckets.get(key)
        if (bucket) {
            bucket.updateTTL()
        } else {
            bucket = new Bucket(streamId, partition)
            this.buckets.set(key, bucket)
        }
        return bucket
    }

    async resetBucket(bucket) {
        await this._storeBucket(bucket)
        bucket.reset()
    }

    async incrementBucket(streamId, partition, records, size) {
        const bucket = this.getBucket(streamId, partition)
        bucket.incrementBucket(records, size)
        if (bucket.getUuid()) {
            await this._storeBucket(bucket)
        }
    }

    async _storeBucket(bucket) {
        const {
            records, size, uuid, streamId, partition, dateCreate
        } = bucket

        const UPDATE_BUCKET = 'UPDATE bucket SET records = ?, size = ? WHERE stream_id = ? AND partition = ? AND date_create = ?'
        const params = [records, size, streamId, partition, new Date(dateCreate).getTime()]
        console.log(UPDATE_BUCKET)
        console.log(params)
        return this.cassandraClient.execute(UPDATE_BUCKET, params, { prepare: true })
    }

    async _getLastNotFullBucketOrCreate(streamId, partition) {
        let bucket

        const INSERT_NEW_BUCKET = 'INSERT INTO bucket (id, stream_id, partition, date_create, records, size) '
                                   + 'VALUES (now(), ?, ?, toTimestamp(now()), 0, 0)'
        const GET_LAST_BUCKET = 'SELECT * FROM bucket WHERE stream_id = ? AND partition = ? LIMIT 1'

        const params = [streamId, partition]
        const resultSet = await this.cassandraClient.execute(GET_LAST_BUCKET, params, {
            prepare: true,
        })

        let bucketIsFull = false
        if (resultSet.rows.length) {
            const row = resultSet.rows[0]
            if (row.records < MAX_BUCKET_RECORDS && row.size < MAX_BUCKET_SIZE) {
                console.log('found bucket')
                console.log(row)
                bucket = row
            } else {
                bucketIsFull = true
            }
        }

        if (!resultSet.rows.length || bucketIsFull) {
            console.log('basket is fill, insert new basket')
            await this.cassandraClient.execute(INSERT_NEW_BUCKET, params, {
                prepare: true,
            })
        }

        return bucket
    }

    async _checkBuckets() {
        console.log('checking buckets')
        const buckets = [...this.buckets.values()]

        for (const bucket of buckets) {
            console.log(bucket)
            const { streamId, partition, records, size } = bucket

            if (!bucket.getUuid()) {
                console.log('empty bucket')
                // eslint-disable-next-line no-await-in-loop
                const found = await this._getLastNotFullBucketOrCreate(streamId, partition)
                if (found) {
                    bucket.updateUuid(found.id.toString())
                    bucket.dateCreate = found.date_create
                    bucket.size = found.size
                    bucket.records = found.records
                }
            }

            if (bucket.getUuid() && (records >= this.maxBucketRecords || size > this.maxBucketSize)) {
                console.log(`full backet: records - ${records}, maxBucketRecords: ${this.maxBucketRecords}, size: ${size}, maxSize: ${this.maxBucketSize}`)
                this.resetBucket(bucket)
            }
        }
    }
}

module.exports = {
    Bucket,
    BucketManager
}
