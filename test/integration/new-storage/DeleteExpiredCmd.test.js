const cassandra = require('cassandra-driver')
const { TimeUuid } = require('cassandra-driver').types

const DeleteExpiredCmd = require('../../../src/new-storage/DeleteExpiredCmd')
const { startBroker, createClient, formConfig } = require('../../utils')

const contactPoints = ['127.0.0.1']
const localDataCenter = 'datacenter1'
const keyspace = 'streamr_dev_v2'

const httpPort = 22341
const wsPort = 22351
const networkPort = 22361
const trackerPort = 22370

const fixtures = async (cassandraClient, streamId, daysAgo) => {
    const timestampDaysAgo = Date.now() - 1000 * 60 * 60 * 24 * daysAgo
    const dateDaysAgo = new Date(timestampDaysAgo)
    const bucketId = TimeUuid.fromDate(dateDaysAgo).toString()
    const query = 'INSERT INTO bucket (stream_id, partition, date_create, id, records, size)'
                  + 'VALUES (?, 0, ?, ?, 1, 1)'
    await cassandraClient.execute(query, [streamId, dateDaysAgo, bucketId], {
        prepare: true
    })

    const insert = 'INSERT INTO stream_data '
        + '(stream_id, partition, bucket_id, ts, sequence_no, publisher_id, msg_chain_id, payload) '
        + 'VALUES (?, 0, ?, ?, 0, ?, ?, ?)'
    await cassandraClient.execute(insert, [
        streamId, bucketId, timestampDaysAgo, 'publisherId', 'chainId', Buffer.from('{}')
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
    let broker
    let client

    let cassandraClient

    let streamId

    beforeEach(async () => {
        cassandraClient = new cassandra.Client({
            contactPoints,
            localDataCenter,
            keyspace,
        })

        broker = await startBroker({
            name: 'broker',
            privateKey: '0xb198d9659173e0957d9cf8f4939d74efb9516465766f2952436103a5f31017b4',
            networkPort,
            trackerPort,
            httpPort,
            wsPort,
            enableCassandra: true
        })
        client = createClient(wsPort, {
            auth: {
                apiKey: 'tester1-api-key'
            },
            orderMessages: false,
        })
        await client.ensureConnected()
    })

    afterEach(async () => {
        await broker.close()
        await client.ensureDisconnected()
        await cassandraClient.shutdown()
    })

    afterAll(() => {
        jest.restoreAllMocks()
    })

    const daysArray = [0, 1, 2, 3]
    daysArray.map(async (days) => {
        test(`keep in database ${days} days of data`, async () => {
            const stream = await client.createStream({
                name: 'DeleteExpiredCmd.test.js-' + Date.now(),
                storageDays: days
            })
            streamId = stream.id

            await fixtures(cassandraClient, streamId, 0)
            await fixtures(cassandraClient, streamId, 1)
            await fixtures(cassandraClient, streamId, 2)
            await fixtures(cassandraClient, streamId, 3)

            const deleteExpiredCmd = new DeleteExpiredCmd(formConfig({
                name: 'name',
                networkPort: 0,
                trackerPort: 0,
                privateKey: '0x09a0c3b1de507fe8655d6d042176c68f7905a77248b0c7c6671d2d94b1fda83e',
                enableCassandra: true
            }))
            await deleteExpiredCmd.run()
            await checkDBCount(cassandraClient, streamId, days)
        })
    })
})
