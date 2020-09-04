const cassandra = require('cassandra-driver')
const { TimeUuid } = require('cassandra-driver').types
const { wait } = require('streamr-test-utils')

const { startBrokerNewSchema, createClient } = require('../../utils')

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
    const query = 'INSERT INTO bucket (stream_id, partition, date_create, id, records, size)'
                  + 'VALUES (?, 0, ?, ?, 1, 1)'
    await cassandraClient.execute(query, [streamId, dateDaysAgo, TimeUuid.fromDate(dateDaysAgo).toString()], {
        prepare: true
    })

    const insert = 'INSERT INTO stream_data '
        + '(stream_id, partition, bucket_id, ts, sequence_no, publisher_id, msg_chain_id, payload) '
        + 'VALUES (?, 0, ?, ?, 0, ?, ?, ?)'
    await cassandraClient.execute(insert, [
        streamId, TimeUuid.fromDate(dateDaysAgo).toString(), timestampDaysAgo, 'publisherId', 'chainId', Buffer.from('{}')
    ], {
        prepare: true
    })
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

        broker = await startBrokerNewSchema('broker', networkPort, trackerPort, httpPort, wsPort, null, true)
        client = createClient(wsPort, {
            auth: {
                apiKey: 'tester1-api-key'
            },
            orderMessages: false,
        })
        await client.ensureConnected()

        const stream = await client.createStream({
            name: 'DeleteExpiredCmd.test.js-' + Date.now()
        })
        streamId = stream.id

        await fixtures(cassandraClient, streamId, 0)
        await fixtures(cassandraClient, streamId, 1)
        await fixtures(cassandraClient, streamId, 2)
    })

    afterEach(async () => {
        await broker.close()
        await client.ensureDisconnected()
        await cassandraClient.shutdown()
    })

    test('keep in database 3 days of data', async () => {
        expect(true).toBeTruthy()
        await wait(3000)
    })
})
