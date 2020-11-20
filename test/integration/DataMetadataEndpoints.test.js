const http = require('http')

const { startTracker, startNetworkNode, startStorageNode, Protocol } = require('streamr-network')
const fetch = require('node-fetch')
const { wait, waitForCondition } = require('streamr-test-utils')
const request = require('supertest')

const { startBroker, createClient } = require('../utils')

const { StreamMessage, MessageID, MessageRef } = Protocol.MessageLayer

const httpPort1 = 12341
const httpPort2 = 12342
const httpPort3 = 12343
const wsPort1 = 12351
const wsPort2 = 12352
const wsPort3 = 12353
const networkPort1 = 12361
const networkPort2 = 12362
const networkPort3 = 12363
const trackerPort = 12370
const broker1Key = '0x241b3f241b110ff7b3e6d52e74fea922006a83e33ff938e6e3cba8a460c02513'
const broker2Key = '0x3816c1d1a81588cecf9ac271a4758ed08f208902c2dcda82ba1a2f458ac23a15'
const broker3Key = '0xe8af31f5c61b64f44adcdab8c5c78a7bc0beea9dbf43af63f80544a1b84ec149'

function createStreamMessage(streamId, idx, prevIdx) {
    return new StreamMessage({
        messageId: new MessageID(streamId, 0, idx, 0, 'publisherId', 'msgChainId'),
        prevMsgRef: prevIdx != null ? new MessageRef(prevIdx, 0) : null,
        content: {
            key: idx,
        },
    })
}

const httpGet = (url) => {
    return new Promise((resolve, reject) => {
        http.get(url, (res) => {
            res.setEncoding('utf8')
            let body = ''
            res.on('data', (chunk) => {
                body += chunk
            })
            res.on('end', () => resolve(body))
        }).on('error', reject)
    })
}

describe('DataMetadataEndpoints', () => {
    let tracker
    let broker1
    let broker2
    let broker3
    let client1
    let client2
    let client3
    // let client4
    let freshStream
    let freshStreamId

    let publisherNode

    beforeAll(async () => {
        tracker = await startTracker({
            host: '127.0.0.1',
            port: trackerPort,
            id: 'tracker'
        })
        publisherNode = await startNetworkNode({
            host: '127.0.0.1',
            port: networkPort1,
            id: 'publisherNode',
            trackers: [tracker.getAddress()]
        })
        publisherNode.start()

        broker1 = await startBroker({
            name: 'broker1',
            privateKey: broker1Key,
            networkPort: networkPort2,
            trackerPort,
            httpPort: httpPort1,
            wsPort: wsPort1,
            enableCassandra: true,
            trackers: [tracker.getAddress()]

        })

        client1 = createClient(wsPort1)
        // await client1.ensureConnected()

        freshStream = await client1.createStream({
            name: 'broker.test.js-' + Date.now()
        })

        freshStreamId = freshStream.id

        await freshStream.grantPermission('stream_get', 'tester2@streamr.com')
        await freshStream.grantPermission('stream_subscribe', 'tester2@streamr.com')
    }, 10 * 1000)

    afterAll(async () => {
        await tracker.stop()
        await client1.ensureDisconnected()
        // await client4.ensureDisconnected()
        await publisherNode.stop()

        await broker1.close()
    })

    it('should fetch empty metadata from Cassandra', async () => {
        const json = await httpGet('http://localhost:12341/api/v1/streams/0/metadata/partitions/0')
        const res = JSON.parse(json)

        expect(res.totalBytes).toEqual(0)
        expect(res.totalMessages).toEqual(0)
        expect(res.firstMessage).toEqual(0)
        expect(res.lastMessage).toEqual(0)
    })

    it('Should publish a single message, store it in Cassandra and return according metadata', async () => {
        await client1.publish(freshStreamId, {
            key: 1
        })
        await wait(3000)

        const json = await httpGet('http://localhost:12341/api/v1/streams/' + freshStreamId + '/metadata/partitions/0')

        const res = JSON.parse(json)

        expect(res.totalBytes).toEqual(184)
        expect(res.totalMessages).toEqual(1)
        expect(res.firstMessage).toEqual(res.lastMessage)
    })

    it('Should publish multiple messages, store them in Cassandra and return according metadata', async () => {
        await client1.publish(freshStreamId, {
            key: 1
        })
        await client1.publish(freshStreamId, {
            key: 2
        })
        await client1.publish(freshStreamId, {
            key: 3
        })
        await client1.publish(freshStreamId, {
            key: 4
        })

        await wait(3000)

        const json = await httpGet('http://localhost:12341/api/v1/streams/' + freshStreamId + '/metadata/partitions/0')

        const res = JSON.parse(json)

        expect(res.totalBytes).toEqual(972)
        expect(res.totalMessages).toEqual(5)
        expect(
            new Date(res.firstMessage).getTime()
        ).toBeLessThan(
            new Date(res.lastMessage).getTime()
        )
    })
})
