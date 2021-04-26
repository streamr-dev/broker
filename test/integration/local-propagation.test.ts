import { AsyncMqttClient } from 'async-mqtt'
import StreamrClient, { Stream } from 'streamr-client'
import { startTracker } from 'streamr-network'
import { wait, waitForCondition } from 'streamr-test-utils'
import { Todo } from '../types'
import { startBroker, createMockUser, createClient, createMqttClient } from '../utils'

const trackerPort = 17711
const httpPort = 17712
const wsPort = 17713
const networkPort = 17701
const mqttPort = 17751

describe('local propagation', () => {
    let tracker: Todo
    let broker: Todo
    const mockUser = createMockUser()
    let client1: StreamrClient
    let client2: StreamrClient
    let freshStream: Stream
    let freshStreamId: string
    let mqttClient1: AsyncMqttClient
    let mqttClient2: AsyncMqttClient

    beforeEach(async () => {
        tracker = await startTracker({
            host: '127.0.0.1',
            port: trackerPort,
            id: 'tracker'
        })

        broker = await startBroker({
            name: 'broker1',
            privateKey: '0xfe77283a570fda0e581897b18d65632c438f0d00f9440183119c1b7e4d5275e1',
            networkPort,
            trackerPort,
            httpPort,
            wsPort,
            mqttPort
        })

        client1 = createClient(wsPort, mockUser.privateKey)
        client2 = createClient(wsPort, mockUser.privateKey)

        mqttClient1 = createMqttClient(mqttPort, 'localhost', mockUser.privateKey)
        mqttClient2 = createMqttClient(mqttPort, 'localhost', mockUser.privateKey)

        freshStream = await client1.createStream({
            name: 'local-propagation.test.js-' + Date.now()
        })
        freshStreamId = freshStream.id

        await wait(3000)
    }, 10 * 1000)

    afterEach(async () => {
        await Promise.all([
            tracker.stop(),
            client1.ensureDisconnected(),
            client2.ensureDisconnected(),
            mqttClient2.end(true),
            mqttClient1.end(true),
            broker.close()
        ])
    })

    test('local propagation using StreamrClients', async () => {
        const client1Messages: Todo[] = []
        const client2Messages: Todo[] = []

        client1.subscribe({
            stream: freshStreamId
        }, (message, metadata) => {
            client1Messages.push(message)
        })

        client2.subscribe({
            stream: freshStreamId
        }, (message, metadata) => {
            client2Messages.push(message)
        })

        await wait(1000)

        await client1.publish(freshStreamId, {
            key: 1
        })
        await client1.publish(freshStreamId, {
            key: 2
        })
        await client1.publish(freshStreamId, {
            key: 3
        })

        await waitForCondition(() => client2Messages.length === 3)
        await waitForCondition(() => client1Messages.length === 3)

        expect(client1Messages).toEqual([
            {
                key: 1
            },
            {
                key: 2
            },
            {
                key: 3
            },
        ])

        expect(client2Messages).toEqual([
            {
                key: 1
            },
            {
                key: 2
            },
            {
                key: 3
            },
        ])
    })

    test('local propagation using mqtt clients', async () => {
        const client1Messages: Todo[] = []
        const client2Messages: Todo[] = []

        await waitForCondition(() => mqttClient1.connected)
        await waitForCondition(() => mqttClient2.connected)

        await mqttClient1.subscribe(freshStreamId)
        await mqttClient2.subscribe(freshStreamId)

        mqttClient1.on('message', (topic, message) => {
            client1Messages.push(JSON.parse(message.toString()))
        })

        mqttClient2.on('message', (topic, message) => {
            client2Messages.push(JSON.parse(message.toString()))
        })

        await mqttClient1.publish(freshStreamId, 'key: 1', {
            qos: 1
        })

        await waitForCondition(() => client1Messages.length === 1)
        await waitForCondition(() => client2Messages.length === 1)

        await mqttClient2.publish(freshStreamId, 'key: 2', {
            qos: 1
        })

        await waitForCondition(() => client1Messages.length === 2)
        await waitForCondition(() => client2Messages.length === 2)

        expect(client1Messages).toEqual([
            {
                mqttPayload: 'key: 1'
            },
            {
                mqttPayload: 'key: 2'
            }
        ])

        expect(client2Messages).toEqual([
            {
                mqttPayload: 'key: 1'
            },
            {
                mqttPayload: 'key: 2'
            }
        ])
    }, 10000)

    test('local propagation using StreamrClients and mqtt clients', async () => {
        const client1Messages: Todo[] = []
        const client2Messages: Todo[] = []
        const client3Messages: Todo[] = []
        const client4Messages: Todo[] = []

        await waitForCondition(() => mqttClient1.connected)
        await waitForCondition(() => mqttClient2.connected)

        await mqttClient1.subscribe(freshStreamId)
        await mqttClient2.subscribe(freshStreamId)

        mqttClient1.on('message', (topic, message) => {
            client1Messages.push(JSON.parse(message.toString()))
        })

        mqttClient2.on('message', (topic, message) => {
            client2Messages.push(JSON.parse(message.toString()))
        })

        client1.subscribe({
            stream: freshStreamId
        }, (message, metadata) => {
            client3Messages.push(message)
        })

        client2.subscribe({
            stream: freshStreamId
        }, (message, metadata) => {
            client4Messages.push(message)
        })

        await wait(1000)

        await mqttClient1.publish(freshStreamId, JSON.stringify({
            key: 1
        }), {
            qos: 1
        })

        await waitForCondition(() => client1Messages.length === 1)
        await waitForCondition(() => client2Messages.length === 1)
        await waitForCondition(() => client3Messages.length === 1)
        await waitForCondition(() => client4Messages.length === 1)

        await mqttClient2.publish(freshStreamId, JSON.stringify({
            key: 2
        }), {
            qos: 1
        })

        await waitForCondition(() => client1Messages.length === 2)
        await waitForCondition(() => client2Messages.length === 2)
        await waitForCondition(() => client3Messages.length === 2)
        await waitForCondition(() => client4Messages.length === 2)

        await client1.publish(freshStreamId, {
            key: 3
        })

        await wait(500)

        await client2.publish(freshStreamId, {
            key: 4
        })

        await waitForCondition(() => client1Messages.length === 4)
        await waitForCondition(() => client2Messages.length === 4)
        await waitForCondition(() => client3Messages.length === 4)
        await waitForCondition(() => client4Messages.length === 4)

        expect(client1Messages).toEqual([
            {
                key: 1
            },
            {
                key: 2
            },
            {
                key: 3
            },
            {
                key: 4
            },
        ])

        expect(client2Messages).toEqual([
            {
                key: 1
            },
            {
                key: 2
            },
            {
                key: 3
            },
            {
                key: 4
            },
        ])

        expect(client3Messages).toEqual([
            {
                key: 1
            },
            {
                key: 2
            },
            {
                key: 3
            },
            {
                key: 4
            },
        ])

        expect(client4Messages).toEqual([
            {
                key: 1
            },
            {
                key: 2
            },
            {
                key: 3
            },
            {
                key: 4
            },
        ])
    }, 10000)
})
