const cassandra = require('cassandra-driver')
const { TimeUuid } = require('cassandra-driver').types
const toArray = require('stream-to-array')
const { StreamMessage, StreamMessageV31 } = require('streamr-client-protocol').MessageLayer
const { waitForCondition, wait } = require('streamr-test-utils')

const BatchManager = require('../../../src/new-storage/BatchManager')
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

describe('BatchManager', () => {
    let batchManager
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
        batchManager = new BatchManager(cassandraClient, {
            logErrors: true,
            batchMaxSize: 10000,
            batchMaxRecords: 10,
            batchCloseTimeout: 1000,
            batchMaxRetries: 64
        })

        streamId = `stream-id-${Date.now()}-${streamIdx}`
        streamIdx += 1
        bucketId = TimeUuid.fromDate(new Date()).toString()
    })

    afterEach(async () => {
        batchManager.stop()
        await cassandraClient.shutdown()
    })

    test('move full batch to pendingBatches', async () => {
        expect(Object.values(batchManager.batches)).toHaveLength(0)
        expect(Object.values(batchManager.pendingBatches)).toHaveLength(0)

        let i = 0
        let msg = buildMsg(streamId, 0, (i + 1) * 1000, i, 'publisher1')
        batchManager.store(bucketId, msg)

        expect(Object.values(batchManager.batches)).toHaveLength(1)
        expect(Object.values(batchManager.pendingBatches)).toHaveLength(0)

        for (i = 1; i < 11; i++) {
            msg = buildMsg(streamId, 0, (i + 1) * 1000, i, 'publisher1')
            batchManager.store(bucketId, msg)
        }

        expect(Object.values(batchManager.batches)).toHaveLength(1)
        expect(Object.values(batchManager.pendingBatches)).toHaveLength(1)

        expect(Object.values(batchManager.batches)[0].streamMessages).toHaveLength(1)
        expect(Object.values(batchManager.pendingBatches)[0].streamMessages).toHaveLength(10)
    })

    test('pendingBatches are inserted', async () => {
        const batch = new Batch(bucketId, 10, 10, 1000, 10)
        const msg = buildMsg(streamId, 0, 1000, 0, 'publisher1')
        batch.push(msg)
        batchManager.pendingBatches[batch.getId()] = batch

        expect(Object.values(batchManager.pendingBatches)).toHaveLength(1)
        expect(Object.values(batchManager.pendingBatches)[0].streamMessages).toHaveLength(1)

        // eslint-disable-next-line no-underscore-dangle
        await batchManager._insert(batch.getId())

        const result = await cassandraClient.execute('SELECT * FROM stream_data_new WHERE stream_id = ? ALLOW FILTERING', [
            streamId
        ])

        expect(result.rows.length).toEqual(1)
    })

    test('when batch changes state, _batchChangedState is triggered', async () => {
        const msg = buildMsg(streamId, 0, 1000, 0, 'publisher1')
        batchManager.store(bucketId, msg)
        const batchChangedStateSpy = jest.spyOn(batchManager, '_batchChangedState')

        const batch = batchManager.batches[bucketId]
        batch.setClose(true)
        expect(batchChangedStateSpy).toHaveBeenCalledWith(bucketId, batch.getId(), Batch.states.CLOSED, 82, 1)
        batchChangedStateSpy.mockClear()

        await waitForCondition(() => batchChangedStateSpy.mock.calls.length === 1)
        expect(batchChangedStateSpy).toHaveBeenCalledWith(bucketId, batch.getId(), Batch.states.PENDING, 82, 1)
    })
})
