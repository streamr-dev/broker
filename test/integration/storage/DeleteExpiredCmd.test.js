const cassandra = require('cassandra-driver')
const { TimeUuid } = require('cassandra-driver').types

const DeleteExpiredCmd = require('../../../src/storage/DeleteExpiredCmd')
const { createClient } = require('../../utils')

const contactPoints = ['127.0.0.1']
const localDataCenter = 'datacenter1'
const keyspace = 'streamr_dev_v2'

const DAY_IN_MS = 1000 * 60 * 60 * 24

const insertBucket = async (cassandraClient, streamId, dateCreate) => {
    const bucketId = TimeUuid.fromDate(dateCreate).toString()
    const query = 'INSERT INTO bucket (stream_id, partition, date_create, id, records, size)'
        + 'VALUES (?, 0, ?, ?, 1, 1)'
    await cassandraClient.execute(query, [streamId, dateCreate, bucketId], {
        prepare: true
    })
    return bucketId
}

const insertData = async (cassandraClient, streamId, bucketId, ts) => {
    const insert = 'INSERT INTO stream_data '
        + '(stream_id, partition, bucket_id, ts, sequence_no, publisher_id, msg_chain_id, payload) '
        + 'VALUES (?, 0, ?, ?, 0, ?, ?, ?)'
    await cassandraClient.execute(insert, [
        streamId, bucketId, ts, 'publisherId', 'msgChainId', Buffer.from('{}')
    ], {
        prepare: true
    })
}

const checkDBCount = async (cassandraClient, streamId, days) => {
    const countBuckets = 'SELECT COUNT(*) FROM bucket WHERE stream_id = ? AND partition = 0 ALLOW FILTERING'
    const result = await cassandraClient.execute(countBuckets, [streamId], {
        prepare: true
    })
    expect(result.first().count.low).toEqual(days)

    const countData = 'SELECT COUNT(*) FROM stream_data WHERE stream_id = ? AND partition = 0 ALLOW FILTERING'
    const resultData = await cassandraClient.execute(countData, [streamId], {
        prepare: true
    })
    expect(resultData.first().count.low).toEqual(days)
}

describe('DeleteExpiredCmd', () => {
    let client
    let cassandraClient

    beforeEach(async () => {
        cassandraClient = new cassandra.Client({
            contactPoints,
            localDataCenter,
            keyspace,
        })
        client = createClient(9999, {
            auth: {
                apiKey: 'tester1-api-key'
            },
            orderMessages: false,
        })
    })

    afterEach(async () => {
        await cassandraClient.shutdown()
    })

    const daysArray = [0, 1, 2, 3]
    daysArray.map(async (days) => {
        test(`keep in database ${days} days of data`, async () => {
            const id = 'sandbox/DeleteExpiredCmd.test.js-' + Date.now()
            const stream = await client.createStream({
                id,
                name: id,
                storageDays: days
            })
            const streamId = stream.id

            const bucketId1 = await insertBucket(cassandraClient, streamId, new Date(Date.now() - 0 * DAY_IN_MS))
            const bucketId2 = await insertBucket(cassandraClient, streamId, new Date(Date.now() - 1 * DAY_IN_MS))
            const bucketId3 = await insertBucket(cassandraClient, streamId, new Date(Date.now() - 2 * DAY_IN_MS))
            const bucketId4 = await insertBucket(cassandraClient, streamId, new Date(Date.now() - 3 * DAY_IN_MS))

            await insertData(cassandraClient, streamId, bucketId1, new Date(Date.now() - 0 * DAY_IN_MS))
            await insertData(cassandraClient, streamId, bucketId2, new Date(Date.now() - 1 * DAY_IN_MS))
            await insertData(cassandraClient, streamId, bucketId3, new Date(Date.now() - 2 * DAY_IN_MS))
            await insertData(cassandraClient, streamId, bucketId4, new Date(Date.now() - 3 * DAY_IN_MS))

            const deleteExpiredCmd = new DeleteExpiredCmd({
                streamrBaseUrl: 'http://localhost:8081/streamr-core',
                cassandraUsername: '',
                cassandraPassword: '',
                cassandraHosts: ['localhost'],
                cassandraDatacenter: 'datacenter1',
                cassandraKeyspace: 'streamr_dev_v2',
                dryRun: false
            })
            await deleteExpiredCmd.run()
            await checkDBCount(cassandraClient, streamId, days)
        }, 10 * 1000)
    })
})
