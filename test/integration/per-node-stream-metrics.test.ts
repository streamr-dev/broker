import StreamrClient, { Stream, StreamOperation } from 'streamr-client'
import {startTracker, Tracker} from 'streamr-network'
import { Todo } from '../types'
import { startBroker, createClient, STREAMR_DOCKER_DEV_HOST } from '../utils'
import { Wallet } from 'ethers'

const httpPort = 47741
const wsPort = 47742
const networkPort1 = 47743
const networkPort2 = 47744
const trackerPort = 47745

const fillMetrics = async (client: StreamrClient, count: number, nodeAddress: string, source: string) => {
    const sourceStream = nodeAddress + '/streamr/node/metrics/' + source
    const mockDate = new Date('2020-01-01').getTime()

    const promises = []

    for (let i = 0; i < count; i++) {
        const ts = mockDate + (i * 1000)

        const mockReport = {
            peerName: nodeAddress,
            peerId: nodeAddress,
            broker: {
                messagesToNetworkPerSec: 0,
                bytesToNetworkPerSec: 0,
                messagesFromNetworkPerSec: 0,
                bytesFromNetworkPerSec: 0,
            },
            network: {
                avgLatencyMs: 0,
                bytesToPeersPerSec: 0,
                bytesFromPeersPerSec: 0,
                connections: 0,
            },
            storage: {
                bytesWrittenPerSec: 0,
                bytesReadPerSec: 0,
            },

            startTime: 0,
            currentTime: ts,
            timestamp: ts
        }

        promises.push(client.publish(sourceStream, mockReport))
    }

    return Promise.allSettled(promises)
}

describe('metricsStream', () => {
    let tracker: Tracker
    let broker1: Todo
    let storageNode: Todo
    let client1: StreamrClient
    let legacyStream: Stream
    let nodeAddress: string
    let client2: StreamrClient
    beforeEach(async () => {
        const tmpAccount = Wallet.createRandom()
        const storageNodeAccount = Wallet.createRandom()
        const storageNodeRegistry = [{
            address: storageNodeAccount.address,
            url: `http://127.0.0.1:${httpPort}`
        }]
        nodeAddress = tmpAccount.address

        client1 = createClient(wsPort, Wallet.createRandom().privateKey)
        legacyStream = await client1.getOrCreateStream({
            name: 'per-node-stream-metrics.test.js-legacyStream'
        })

        await legacyStream.grantPermission('stream_get' as StreamOperation, undefined)
        await legacyStream.grantPermission('stream_publish' as StreamOperation, nodeAddress)

        tracker = await startTracker({
            host: '127.0.0.1',
            port: trackerPort,
            id: 'tracker'
        })

        storageNode = await startBroker({
            name: 'storageNode',
            privateKey: storageNodeAccount.privateKey,
            networkPort: networkPort2,
            trackerPort,
            httpPort,
            enableCassandra: true,
            storageNodeRegistry
        })

        broker1 = await startBroker({
            name: 'broker1',
            privateKey: tmpAccount.privateKey,
            networkPort: networkPort1,
            trackerPort,
            wsPort,
            reporting: {
                sentry: null,
                streamr: {
                    streamId: legacyStream.id
                },
                intervalInSeconds: 1,
                perNodeMetrics: {
                    enabled: true,
                    wsUrl: `ws://127.0.0.1:${wsPort}/api/v1/ws`,
                    httpUrl: `http://${STREAMR_DOCKER_DEV_HOST}/api/v1`,
                    intervals: {
                        sec: 1000,
                        min: 1000,
                        hour: 1000,
                        day: 1000
                    },
                    storageNode: storageNodeAccount.address
                }
            },
            storageNodeRegistry
        })

        client2 = createClient(wsPort, tmpAccount.privateKey)
    }, 30 * 1000)

    afterEach(async () => {
        await Promise.allSettled([
            tracker.stop(),
            broker1.close(),
            storageNode.close(),
            client1.ensureDisconnected(),
            client2.ensureDisconnected()
        ])
    }, 30 * 1000)

    it('should ensure the legacy metrics endpoint still works properly', (done) => {
        client1.subscribe({
            stream: legacyStream.id,
        }, (res) => {
            expect(res.peerId).toEqual('broker1')
            done()
        })
    })

    it('should retrieve the last `sec` metrics', (done) => {
        client1.subscribe({
            stream: nodeAddress + '/streamr/node/metrics/sec',
        }, (res) => {
            expect(res.peerName).toEqual('broker1')
            expect(res.broker.messagesToNetworkPerSec).toBeGreaterThanOrEqual(0)
            expect(res.broker.bytesToNetworkPerSec).toBeGreaterThanOrEqual(0)
            expect(res.broker.messagesFromNetworkPerSec).toBeGreaterThanOrEqual(0)
            expect(res.broker.bytesFromNetworkPerSec).toBeGreaterThanOrEqual(0)
            expect(res.network.avgLatencyMs).toBeGreaterThanOrEqual(0)
            expect(res.network.bytesToPeersPerSec).toBeGreaterThanOrEqual(0)
            expect(res.network.bytesFromPeersPerSec).toBeGreaterThanOrEqual(0)
            expect(res.network.connections).toBeGreaterThanOrEqual(0)
            expect(res.storage.bytesWrittenPerSec).toBeGreaterThanOrEqual(0)
            expect(res.storage.bytesReadPerSec).toBeGreaterThanOrEqual(0)
            expect(res.startTime).toBeGreaterThanOrEqual(0)
            expect(res.currentTime).toBeGreaterThanOrEqual(0)
            expect(res.timestamp).toBeGreaterThanOrEqual(0)

            done()
        })
    })

    it('should retrieve the last `min` metrics', (done) => {
        client1.subscribe({
            stream: nodeAddress + '/streamr/node/metrics/min',
        }, (res) => {
            expect(res.peerName).toEqual('broker1')
            expect(res.broker.messagesToNetworkPerSec).toBeGreaterThanOrEqual(0)
            expect(res.broker.bytesToNetworkPerSec).toBeGreaterThanOrEqual(0)
            expect(res.broker.messagesFromNetworkPerSec).toBeGreaterThanOrEqual(0)
            expect(res.broker.bytesFromNetworkPerSec).toBeGreaterThanOrEqual(0)
            expect(res.network.avgLatencyMs).toBeGreaterThanOrEqual(0)
            expect(res.network.bytesToPeersPerSec).toBeGreaterThanOrEqual(0)
            expect(res.network.bytesFromPeersPerSec).toBeGreaterThanOrEqual(0)
            expect(res.network.connections).toBeGreaterThanOrEqual(0)
            expect(res.storage.bytesWrittenPerSec).toBeGreaterThanOrEqual(0)
            expect(res.storage.bytesReadPerSec).toBeGreaterThanOrEqual(0)
            expect(res.startTime).toBeGreaterThanOrEqual(0)
            expect(res.currentTime).toBeGreaterThanOrEqual(0)
            expect(res.timestamp).toBeGreaterThanOrEqual(0)
            done()
        })
        fillMetrics(client2, 60, nodeAddress, 'sec')
    })

    it('should retrieve the last `hour` metrics', (done) => {
        client1.subscribe({
            stream: nodeAddress + '/streamr/node/metrics/hour',
        }, (res) => {
            expect(res.peerName).toEqual('broker1')
            expect(res.broker.messagesToNetworkPerSec).toBeGreaterThanOrEqual(0)
            expect(res.broker.bytesToNetworkPerSec).toBeGreaterThanOrEqual(0)
            expect(res.broker.messagesFromNetworkPerSec).toBeGreaterThanOrEqual(0)
            expect(res.broker.bytesFromNetworkPerSec).toBeGreaterThanOrEqual(0)
            expect(res.network.avgLatencyMs).toBeGreaterThanOrEqual(0)
            expect(res.network.bytesToPeersPerSec).toBeGreaterThanOrEqual(0)
            expect(res.network.bytesFromPeersPerSec).toBeGreaterThanOrEqual(0)
            expect(res.network.connections).toBeGreaterThanOrEqual(0)
            expect(res.storage.bytesWrittenPerSec).toBeGreaterThanOrEqual(0)
            expect(res.storage.bytesReadPerSec).toBeGreaterThanOrEqual(0)
            expect(res.startTime).toBeGreaterThanOrEqual(0)
            expect(res.currentTime).toBeGreaterThanOrEqual(0)
            expect(res.timestamp).toBeGreaterThanOrEqual(0)
            done()
        })
        fillMetrics(client2, 60, nodeAddress, 'min')
    })

    it('should retrieve the last `day` metrics', (done) => {
        client1.subscribe({
            stream: nodeAddress + '/streamr/node/metrics/day',
        }, (res) => {
            expect(res.peerName).toEqual('broker1')
            expect(res.broker.messagesToNetworkPerSec).toBeGreaterThanOrEqual(0)
            expect(res.broker.bytesToNetworkPerSec).toBeGreaterThanOrEqual(0)
            expect(res.broker.messagesFromNetworkPerSec).toBeGreaterThanOrEqual(0)
            expect(res.broker.bytesFromNetworkPerSec).toBeGreaterThanOrEqual(0)
            expect(res.network.avgLatencyMs).toBeGreaterThanOrEqual(0)
            expect(res.network.bytesToPeersPerSec).toBeGreaterThanOrEqual(0)
            expect(res.network.bytesFromPeersPerSec).toBeGreaterThanOrEqual(0)
            expect(res.network.connections).toBeGreaterThanOrEqual(0)
            expect(res.storage.bytesWrittenPerSec).toBeGreaterThanOrEqual(0)
            expect(res.storage.bytesReadPerSec).toBeGreaterThanOrEqual(0)
            expect(res.startTime).toBeGreaterThanOrEqual(0)
            expect(res.currentTime).toBeGreaterThanOrEqual(0)
            expect(res.timestamp).toBeGreaterThanOrEqual(0)
            done()
        })
        fillMetrics(client2, 24, nodeAddress, 'hour')
    })
})
