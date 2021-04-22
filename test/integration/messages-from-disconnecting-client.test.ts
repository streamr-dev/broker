import fetch from 'node-fetch'
import { startTracker, Tracker } from 'streamr-network'
import { Stream, StreamOperation, StreamrClient, Todo } from 'streamr-client'
import { waitForCondition } from 'streamr-test-utils'
import { createClient, startBroker, StorageAssignmentEventManager, waitForStreamPersistedInStorageNode } from '../utils'
import { ethers } from 'ethers'

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
    let tracker: Tracker
    let broker1: Todo
    let broker2: Todo
    let storageBroker: Todo
    let client1: StreamrClient
    let client2: StreamrClient
    let assignmentEventManager: StorageAssignmentEventManager

    let freshStream: Stream

    beforeEach(async () => {
        const engineAndEditorAccount = ethers.Wallet.createRandom()
        const broker1Account = ethers.Wallet.createRandom()
        const broker2Account = ethers.Wallet.createRandom()
        const storageBrokerAccount = ethers.Wallet.createRandom()

        tracker = await startTracker({
            host: '127.0.0.1',
            port: trackerPort,
            id: 'tracker'
        })
        broker1 = await startBroker({
            name: 'broker1',
            privateKey: broker1Account.privateKey,
            networkPort: 13556,
            trackerPort,
            wsPort: wsPort1
        })
        broker2 = await startBroker({
            name: 'broker2',
            privateKey: broker2Account.privateKey,
            networkPort: 13558,
            trackerPort,
            wsPort: wsPort2
        })
        storageBroker = await startBroker({
            name: 'storageBroker',
            privateKey: storageBrokerAccount.privateKey,
            networkPort: 13560,
            trackerPort,
            httpPort,
            enableCassandra: true,
            streamrAddress: engineAndEditorAccount.address,
        })

        client1 = createClient(wsPort1, undefined, {
            storageNode: {
                address: storageBrokerAccount.address,
                url: `http://127.0.0.1:${httpPort}`
            }
        })
        client2 = createClient(wsPort2, undefined)

        assignmentEventManager = new StorageAssignmentEventManager(wsPort1, engineAndEditorAccount)
        await assignmentEventManager.createStream()

        freshStream = await client1.createStream({
            name: 'messages-from-disconnecting-client.test.js-' + Date.now()
        })
        await freshStream.grantPermission(StreamOperation.STREAM_GET, undefined)
        await freshStream.grantPermission(StreamOperation.STREAM_SUBSCRIBE, undefined)
        await assignmentEventManager.addStreamToStorageNode(freshStream.id, storageBrokerAccount.address, client1)
        await waitForStreamPersistedInStorageNode(freshStream.id, 0, '127.0.0.1', httpPort)
    }, 30 * 1000)

    afterEach(async () => {
        await Promise.allSettled([
            client1.ensureDisconnected(),
            client2.ensureDisconnected(),
            broker1.close(),
            broker2.close(),
            storageBroker.close(),
            tracker.stop(),
            assignmentEventManager.close()
        ])
    })

    it('messages from disconnecting client are delivered to other clients and storage node', async () => {
        let client2TotalReceivedMessages = 0
        await client2.subscribe({
            stream: freshStream.id
        }, (_msg) => {
            client2TotalReceivedMessages += 1
        })

        await Promise.allSettled([
            client1.publish(freshStream.id, {
            hello: '1'
            }),
            client1.publish(freshStream.id, {
                hello: '2'
            }),
            client1.publish(freshStream.id, {
                hello: '3'
            }),
        ])
        await client1.disconnect()

        // Verify that non-storage node (broker2) got the expected 3 messages
        await waitForCondition(() => client2TotalReceivedMessages === 3, undefined, undefined,
            () => `received ${client2TotalReceivedMessages} messages, expected 3`)

        // Verify that storage node stored the expected 3 messages
        const url = `http://localhost:${httpPort}/api/v1/streams/${freshStream.id}/data/partitions/0/last?count=10`
        let json: Todo = null
        await waitForCondition(async () => {
            const response = await fetch(url)
            json = await response.json()
            return json.length === 3
        }, 8000, 1000, () => `last response was ${json}`)

        // Verify contents just to make sure
        expect(json.map((j: Todo) => j.content)).toEqual([
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
