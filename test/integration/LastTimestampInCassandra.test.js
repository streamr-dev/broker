const cassandra = require('cassandra-driver')
const uuid = require('uuid')
const { wait } = require('streamr-test-utils')

const { startBroker, createClient } = require('../utils')

const contactPoints = ['127.0.0.1']
const localDataCenter = 'datacenter1'
const keyspace = 'streamr_dev'

const getLastTimeStamp = async (cassandraClient, streamId) => {
    const result = await cassandraClient.execute('SELECT * FROM stream_last_msg WHERE id = ? LIMIT 1', [
        streamId
    ])
    return result && result.rowLength > 0 ? result.rows[0] : undefined
}

const getLastTimeStamps = async (cassandraClient, streamIds) => {
    const result = await cassandraClient.execute('SELECT * FROM stream_last_msg WHERE id IN ?', [
        streamIds
    ])
    return result && result.rowLength > 0 ? result.rows : undefined
}

const httpPort = 12941
const wsPort = 12951
const networkPort = 12961

describe('store last timestamp for each stream with each batch', () => {
    let broker
    let cassandraClient
    let client

    let stream
    let streamId
    let streamName

    beforeAll(async () => {
        broker = await startBroker('broker', networkPort, 30300, httpPort, wsPort, null, true)

        cassandraClient = new cassandra.Client({
            contactPoints,
            localDataCenter,
            keyspace
        })

        streamName = `stream-last-timestamp-${uuid.v4()}`

        client = createClient(wsPort, 'tester1-api-key')

        stream = await client.createStream({
            name: streamName
        })
        streamId = stream.id
    })

    afterAll(async () => {
        await Promise.all([
            await broker.close(),
            await client.ensureDisconnected(),
            await cassandraClient.shutdown()
        ])
    })

    test('expect lastTimestamp to be empty for new stream', async () => {
        const result = await getLastTimeStamp(cassandraClient, streamId)
        expect(result).toBeUndefined()
    })

    test('expect lastTimestamp to be not undefined and greater than now', async () => {
        const now = new Date().toISOString()
        await client.publish(streamId, {
            key: 1
        })

        await wait(2000) // wait storage to store, hopefully
        const result = await getLastTimeStamp(cassandraClient, streamId)

        expect(result.time).not.toBeUndefined()
        expect(Date.parse(result.time)).toBeGreaterThan(Date.parse(now))
    })

    test('expect lastTimestamp to update after each publish', async () => {
        const currentLastTimeStamp = await getLastTimeStamp(cassandraClient, streamId)
        await client.publish(streamId, {
            key: 1
        })

        await wait(2000) // wait storage to store, hopefully
        const result = await getLastTimeStamp(cassandraClient, streamId)

        expect(Date.parse(result.time)).toBeGreaterThan(Date.parse(currentLastTimeStamp.time))
    })

    test('test getting N timestamps', async () => {
        const streamName2 = `stream-last-timestamp-${uuid.v4()}`
        const stream2 = await client.createStream({
            name: streamName2
        })
        const streamId2 = stream2.id

        await client.publish(streamId2, {
            key: 1
        })

        await wait(2000) // wait storage to store, hopefully
        const result = await getLastTimeStamps(cassandraClient, [streamId, streamId2])

        expect(result.length).toEqual(2)
    })
})

