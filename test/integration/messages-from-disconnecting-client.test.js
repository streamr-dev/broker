const { startTracker } = require('streamr-network')
const { waitForCondition } = require('streamr-test-utils')
const fetch = require('node-fetch')

const { startBroker, createClient } = require('../utils')

const trackerPort = 13555
const wsPort1 = 13557
const wsPort2 = 13559
const httpPort = 13561

/**
 * This test is in response to bug report NET-166. In this test we verify that messages,
 * published by a client that immediately thereafter disconnects, are successfully
 * delivered to other network participants.
 */
describe('messages from disconnecting client', () => {
    let tracker
    let broker1
    let broker2
    let storageBroker
    let client1
    let client2

    let freshStream

    beforeEach(async () => {
        tracker = await startTracker({
            host: '127.0.0.1',
            port: trackerPort,
            id: 'tracker'
        })
        broker1 = await startBroker({
            name: 'broker1',
            privateKey: '0x982252e41a9d6d719faec914f26f45b2fae0affba6593dfdd1036c2ef01f28d2',
            networkPort: 13556,
            trackerPort,
            wsPort: wsPort1
        })
        broker2 = await startBroker({
            name: 'broker2',
            privateKey: '0xc80ee48d8627a9ba31ee473af482310933e6e94623a25b76b5a8340816059c98',
            networkPort: 13558,
            trackerPort,
            wsPort: wsPort2
        })
        storageBroker = await startBroker({
            name: 'storageBroker',
            privateKey: '0x57c5c8d1936e9d234bc1273e3a180417a2f8e875a864ec55b320d60f50257057',
            networkPort: 13560,
            trackerPort,
            httpPort,
            enableCassandra: true
        })

        client1 = createClient(wsPort1)
        client2 = createClient(wsPort2)

        freshStream = await client1.createStream({
            name: 'SubscriptionManager.test.js-' + Date.now()
        })
        await freshStream.grantPermission('stream_get', null)
        await freshStream.grantPermission('stream_subscribe', null)
    }, 10 * 1000)

    afterEach(async () => {
        await Promise.allSettled([
            client1.ensureDisconnected(),
            client2.ensureDisconnected(),
            broker1.close(),
            broker2.close(),
            storageBroker.close(),
            tracker.stop()
        ])
    })

    it('messages from disconnecting client are delivered to other clients and storage node', async () => {
        client1.publish(freshStream.id, {
            hello: '1'
        })
        client1.publish(freshStream.id, {
            hello: '2'
        })
        client1.publish(freshStream.id, {
            hello: '3'
        })
        client1.ensureDisconnected()

        // Verify that non-storage node (broker2) got the expected 3 messages
        let client2TotalReceivedMessages = 0
        client2.subscribe({
            stream: freshStream.id
        }, (_msg) => {
            client2TotalReceivedMessages += 1
        })
        await waitForCondition(() => client2TotalReceivedMessages === 3)

        // Verify that storage node stored the expected 3 messages
        const url = `http://localhost:${httpPort}/api/v1/streams/${freshStream.id}/data/partitions/0/last?count=10`
        let json = null
        await waitForCondition(async () => {
            const response = await fetch(url)
            json = await response.json()
            return json.length === 3
        }, 8000, 1000, () => `last response was ${json}`)

        // Verify contents just to make sure
        expect(json.map((j) => j.content)).toEqual([
            {
                hello: '1'
            },
            {
                hello: '2'
            },
            {
                hello: '3'
            }
        ])
    }, 10 * 1000)
})
