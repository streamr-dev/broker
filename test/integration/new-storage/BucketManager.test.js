const cassandra = require('cassandra-driver')
const { waitForCondition, wait } = require('streamr-test-utils')

const BucketManager = require('../../../src/new-storage/BucketManager')

const contactPoints = ['127.0.0.1']
const localDataCenter = 'datacenter1'
const keyspace = 'streamr_dev'

describe('BucketManager', () => {
    let bucketManager
    let streamId
    let cassandraClient
    let streamIdx = 1

    beforeEach(async () => {
        cassandraClient = new cassandra.Client({
            contactPoints,
            localDataCenter,
            keyspace,
        })

        await cassandraClient.connect()
        bucketManager = new BucketManager(cassandraClient, {
            logErrors: true,
            checkFullBucketsTimeout: 1000,
            storeBucketsTimeout: 1000,
            maxBucketSize: 10 * 300,
            maxBucketRecords: 10,
            bucketKeepAliveSeconds: 60
        })

        streamId = `stream-id-${Date.now()}-${streamIdx}`
        streamIdx += 1
    })

    afterEach(async () => {
        bucketManager.stop()
        await cassandraClient.shutdown()
    })

    test('calling getBucketId() will try to find bucket in database and then create if not found and store in database', async () => {
        const timestamp = Date.now()

        const storeBucketsSpy = jest.spyOn(bucketManager, '_storeBuckets')

        expect(Object.values(bucketManager.streams)).toHaveLength(0)
        expect(Object.values(bucketManager.buckets)).toHaveLength(0)

        expect(bucketManager.getBucketId(streamId, 0, timestamp)).toBeUndefined()
        let result = await cassandraClient.execute('SELECT * FROM bucket WHERE stream_id = ? ALLOW FILTERING', [
            streamId
        ])
        expect(result.rows.length).toEqual(0)

        // first time we call in constructor, second after timeout
        await waitForCondition(() => storeBucketsSpy.mock.calls.length === 2)
        expect(storeBucketsSpy).toHaveBeenCalled()

        const foundBucketId = bucketManager.getBucketId(streamId, 0, timestamp)
        expect(foundBucketId).not.toBeUndefined()
        expect(bucketManager.buckets[foundBucketId].size).toEqual(0)
        expect(bucketManager.buckets[foundBucketId].records).toEqual(0)
        expect(bucketManager.buckets[foundBucketId].isStored()).toBeTruthy()

        bucketManager.incrementBucket(foundBucketId, 3)
        bucketManager.incrementBucket(foundBucketId, 3)
        expect(bucketManager.buckets[foundBucketId].size).toEqual(6)
        expect(bucketManager.buckets[foundBucketId].records).toEqual(2)
        expect(bucketManager.buckets[foundBucketId].isStored()).toBeFalsy()

        await waitForCondition(() => bucketManager.buckets[foundBucketId].isStored())
        result = await cassandraClient.execute('SELECT * FROM bucket WHERE stream_id = ? ALLOW FILTERING', [
            streamId
        ])
        const row = result.first()

        expect(row).not.toBeUndefined()
        expect(row.stream_id).toEqual(streamId)
        expect(row.partition).toEqual(0)
        expect(row.records).toEqual(2)
        expect(row.size).toEqual(6)
    })

    test('calling getBucketId() updates last know min timestamp and resets it to undefined when bucket is found', async () => {
        const timestamp = Date.now()

        expect(bucketManager.getBucketId(streamId, 0, timestamp)).toBeUndefined()
        expect(bucketManager.streams[`${streamId}-0`].minTimestamp).toEqual(timestamp)

        await waitForCondition(() => bucketManager.getBucketId(streamId, 0, timestamp) !== undefined)
        expect(bucketManager.streams[`${streamId}-0`].minTimestamp).toBeUndefined()

        // future timestamp will give latest not full bucket
        expect(bucketManager.getBucketId(streamId, 0, timestamp + 600)).not.toBeUndefined()
        expect(bucketManager.streams[`${streamId}-0`].minTimestamp).toBeUndefined()
    })

    test('calling getBucketId() with timestamp in the past, will try to find correct bucket and then create buckets in the past', async () => {
        const timestamp = Date.now()
        const timestamp5ago = timestamp - 5 * 60 * 1000 // 5 minutes

        // find or create bucketId for NOW timestamp
        expect(bucketManager.getBucketId(streamId, 0, timestamp)).toBeUndefined()
        await waitForCondition(() => bucketManager.getBucketId(streamId, 0, timestamp) !== undefined)
        const lastBucketId = bucketManager.getBucketId(streamId, 0, timestamp)

        // find or create bucketId for NOW - 5 minutes timestamp
        expect(bucketManager.getBucketId(streamId, 0, timestamp5ago)).toBeUndefined()
        await waitForCondition(() => bucketManager.getBucketId(streamId, 0, timestamp5ago) !== undefined)
        const bucketId5minAgo = bucketManager.getBucketId(streamId, 0, timestamp5ago)

        // bucketId is not latest
        expect(lastBucketId).not.toEqual(bucketId5minAgo)

        // set stored = false
        bucketManager.incrementBucket(lastBucketId, 1)
        bucketManager.incrementBucket(bucketId5minAgo, 1)

        await waitForCondition(() => bucketManager.buckets[lastBucketId].isStored())
        await waitForCondition(() => bucketManager.buckets[bucketId5minAgo].isStored())

        await wait(5000)

        // get latest sorted
        const lastBuckets = await bucketManager.getLastBuckets(streamId, 0, 5)
        expect(lastBuckets).toHaveLength(2)
        expect(lastBuckets[0].getId()).toEqual(lastBucketId)
        expect(lastBuckets[1].getId()).toEqual(bucketId5minAgo)

        // get latest from
        const lastBucketsFrom = await bucketManager.getBucketsByTimestamp(streamId, 0, timestamp5ago)
        expect(lastBucketsFrom).toHaveLength(2)
        expect(lastBucketsFrom[0].getId()).toEqual(lastBucketId)
        expect(lastBucketsFrom[1].getId()).toEqual(bucketId5minAgo)

        // get latest from-to
        const lastBucketsFromTo = await bucketManager.getBucketsByTimestamp(streamId, 0, timestamp5ago, timestamp)
        expect(lastBucketsFromTo).toHaveLength(2)
        expect(lastBucketsFromTo[0].getId()).toEqual(lastBucketId)
        expect(lastBucketsFromTo[1].getId()).toEqual(bucketId5minAgo)
    }, 20000)
})
