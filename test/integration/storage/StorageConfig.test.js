const { startTracker } = require('streamr-network')
const cassandra = require('cassandra-driver')
const ethers = require('ethers')
const fetch = require('node-fetch')
const { waitForCondition } = require('streamr-test-utils')
const { StreamMessage } = require('streamr-network').Protocol.MessageLayer

const { startBroker, createClient, addStreamToStorageNode } = require('../../utils')

const contactPoints = ['127.0.0.1']
const localDataCenter = 'datacenter1'
const keyspace = 'streamr_dev_v2'

const NODE_HOST = '127.0.0.1'
const STREAMR_URL = 'http://127.0.0.1'
const API_URL = `${STREAMR_URL}/api/v1`
const HTTP_PORT = 17770
const WS_PORT = 17771
const TRACKER_PORT = 17772
const STORAGE_NODE_PORT = 17773
const BROKER_PORT = 17774

const isPersistent = async (streamId, partition = 0) => {
    const response = await fetch(`http://${NODE_HOST}:${HTTP_PORT}/api/v1/streams/${encodeURIComponent(streamId)}/storage/partitions/${partition}`)
    const json = await response.json()
    return json.persistent
}

describe('StorageConfig', () => {
    let cassandraClient
    let tracker
    let storageNode
    let broker
    let client
    let stream
    const publisherAccount = ethers.Wallet.createRandom()
    const storageNodeAccount = ethers.Wallet.createRandom()
    const brokerAccount = ethers.Wallet.createRandom()

    beforeAll(async () => {
        cassandraClient = new cassandra.Client({
            contactPoints,
            localDataCenter,
            keyspace,
        })
    })

    afterAll(() => {
        cassandraClient.shutdown()
    })

    beforeEach(async () => {
        tracker = await startTracker({
            host: NODE_HOST,
            port: TRACKER_PORT,
            id: 'tracker'
        })
        storageNode = await startBroker({
            name: 'storageNode',
            privateKey: storageNodeAccount.privateKey,
            networkPort: STORAGE_NODE_PORT,
            trackerPort: TRACKER_PORT,
            httpPort: HTTP_PORT,
            streamrUrl: STREAMR_URL,
            enableCassandra: true
        })
        broker = await startBroker({
            name: 'broker',
            privateKey: brokerAccount.privateKey,
            networkPort: BROKER_PORT,
            trackerPort: TRACKER_PORT,
            wsPort: WS_PORT,
            streamrUrl: STREAMR_URL,
            enableCassandra: false
        })
        client = createClient(WS_PORT, {
            auth: {
                privateKey: publisherAccount.privateKey
            },
            restUrl: API_URL
        })
    })

    afterEach(async () => {
        await Promise.allSettled([storageNode.close(), broker.close(), tracker.stop()])
    })

    it('when client publishes a message, it is written to the store', async () => {
        stream = await client.createStream({
            id: publisherAccount.address + '/StorageConfigTest/' + Date.now()
        })
        await addStreamToStorageNode(stream.id, storageNodeAccount.address, client)
        await waitForCondition(() => isPersistent(stream.id))
        const publishMessage = await client.publish(stream.id, {
            foo: 'bar'
        })
        await waitForCondition(async () => {
            const result = await cassandraClient.execute('SELECT COUNT(*) FROM stream_data WHERE stream_id = ? ALLOW FILTERING', [stream.id])
            return (result.first().count > 0)
        })
        const result = await cassandraClient.execute('SELECT * FROM stream_data WHERE stream_id = ? ALLOW FILTERING', [stream.id])
        const storeMessage = StreamMessage.deserialize(JSON.parse(result.first().payload.toString()))
        expect(storeMessage.messageId).toEqual(publishMessage.streamMessage.messageId)
    }, 10000)
})
