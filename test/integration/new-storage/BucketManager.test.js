const cassandra = require('cassandra-driver')
const { TimeUuid } = require('cassandra-driver').types
const toArray = require('stream-to-array')
const { StreamMessage, StreamMessageV31 } = require('streamr-client-protocol').MessageLayer
const { waitForCondition, wait } = require('streamr-test-utils')

const BucketManager = require('../../../src/new-storage/BucketManager')
const Batch = require('../../../src/new-storage/Batch')

const contactPoints = ['127.0.0.1']
const localDataCenter = 'datacenter1'
const keyspace = 'streamr_dev'

function buildMsg(
    streamId,
    streamPartition,
    timestamp,
    sequenceNumber,
    publisherId = 'publisher',
    msgChainId = '1',
    content = {}
) {
    return StreamMessage.create(
        [streamId, streamPartition, timestamp, sequenceNumber, publisherId, msgChainId],
        null,
        StreamMessage.CONTENT_TYPES.MESSAGE,
        StreamMessage.ENCRYPTION_TYPES.NONE,
        content,
        StreamMessage.SIGNATURE_TYPES.NONE,
        null,
    )
}

describe('BucketManager', () => {
    let bucketManager
    let streamId
    let cassandraClient
    let streamIdx = 1
    let bucketId

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
        bucketId = TimeUuid.fromDate(new Date()).toString()
    })

    afterEach(async () => {
        bucketManager.stop()
        await cassandraClient.shutdown()
    })

    test('calling getBucketId() will try to find bucket in database and then create if not found', async () => {
        const timestamp = Date.now()

        const storeBucketsSpy = jest.spyOn(bucketManager, '_storeBuckets')
        const checkFullBucketsSpy = jest.spyOn(bucketManager, '_checkFullBuckets')

        expect(Object.values(bucketManager.streams)).toHaveLength(0)
        expect(Object.values(bucketManager.buckets)).toHaveLength(0)

        let result = await cassandraClient.execute('SELECT * FROM bucket WHERE stream_id = ? ALLOW FILTERING', [
            streamId
        ])
        expect(result.rows.length).toEqual(0)
        expect(bucketManager.getBucketId(streamId, 0, timestamp)).toBeUndefined()

        await waitForCondition(() => checkFullBucketsSpy.mock.calls.length === 2)
        expect(checkFullBucketsSpy).toHaveBeenCalled()

        await waitForCondition(() => storeBucketsSpy.mock.calls.length === 2)
        expect(checkFullBucketsSpy).toHaveBeenCalled()

        const foundBucketId = bucketManager.getBucketId(streamId, 0, timestamp)
        expect(foundBucketId).not.toBeUndefined()
        expect(bucketManager.buckets[foundBucketId].size).toEqual(0)
        expect(bucketManager.buckets[foundBucketId].records).toEqual(0)
        expect(bucketManager.buckets[foundBucketId].isStored()).toBeTruthy()

        bucketManager.incrementBucket(foundBucketId, 3, 9)
        expect(bucketManager.buckets[foundBucketId].size).toEqual(3)
        expect(bucketManager.buckets[foundBucketId].records).toEqual(9)
        expect(bucketManager.buckets[foundBucketId].isStored()).toBeFalsy()

        await waitForCondition(() => bucketManager.buckets[foundBucketId].isStored())
        result = await cassandraClient.execute('SELECT * FROM bucket WHERE stream_id = ? ALLOW FILTERING', [
            streamId
        ])
        expect(result.rows.length).toEqual(1)
    })

    test('calling getBucketId() updates last know min timestamp, resets to undefined when bucket is found', async () => {
        const timestamp = Date.now()

        expect(bucketManager.streams[`${streamId}-0`]).toBeUndefined()
        expect(bucketManager.getBucketId(streamId, 0, timestamp)).toBeUndefined()
        expect(bucketManager.streams[`${streamId}-0`].timestamps.min).toEqual(timestamp)

        await waitForCondition(() => bucketManager.getBucketId(streamId, 0, timestamp) !== undefined)
        expect(bucketManager.streams[`${streamId}-0`].timestamps.min).toBeUndefined()
        expect(bucketManager.getBucketId(streamId, 0, timestamp)).not.toBeUndefined()
        expect(bucketManager.streams[`${streamId}-0`].timestamps.min).toBeUndefined()

        expect(bucketManager.getBucketId(streamId, 0, timestamp + 1)).not.toBeUndefined()
        expect(bucketManager.streams[`${streamId}-0`].timestamps.min).toBeUndefined()
    })

    test('calling getBucketId() creates buckets in the past', async () => {
        const timestamp = Date.now()
        const timestamp5ago = timestamp - 5 * 60 * 1000 // 5 minutes

        expect(bucketManager.getBucketId(streamId, 0, timestamp)).toBeUndefined()
        await waitForCondition(() => bucketManager.getBucketId(streamId, 0, timestamp) !== undefined)
        const lastBucketId = bucketManager.getBucketId(streamId, 0, timestamp)

        expect(bucketManager.getBucketId(streamId, 0, timestamp5ago)).toBeUndefined()
        await waitForCondition(() => bucketManager.getBucketId(streamId, 0, timestamp5ago) !== undefined)
        const bucketId5minAgo = bucketManager.getBucketId(streamId, 0, timestamp5ago)
        expect(lastBucketId).not.toEqual(bucketId5minAgo)

        // set stored = false
        bucketManager.incrementBucket(lastBucketId, 1, 1)
        bucketManager.incrementBucket(bucketId5minAgo, 1, 1)

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
