const { startTracker } = require('streamr-network')
const fetch = require('node-fetch')
const { wait, waitForCondition } = require('streamr-test-utils')

const { startBroker, createClient } = require('../utils')

const httpPort1 = 12341
const wsPort1 = 12351
const networkPort1 = 12361
const trackerPort = 12970
const broker1Key = '0x241b3f241b110ff7b3e6d52e74fea922006a83e33ff938e6e3cba8a460c02513'

describe('metricsStream', () => {
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

    beforeAll(async () => {
        tracker = await startTracker({
            host: '127.0.0.1',
            port: trackerPort,
            id: 'tracker'
        })

        broker1 = await startBroker({
            name: 'broker1',
            privateKey: broker1Key,
            networkPort: networkPort1,
            trackerPort,
            httpPort: httpPort1,
            wsPort: wsPort1,
            reporting: {
                sentry: null,
                streamr: null,
                intervalInSeconds: 10,
                perNodeMetrics: {
                    enabled: true,
                    wsUrl: 'ws://127.0.0.1:12351/api/v1/ws',
                    httpUrl: 'http://localhost/api/v1'
                }
            }
        })

        client1 = createClient(wsPort1)
    })

    afterAll(async () => {
        await tracker.stop()
        await client1.ensureDisconnected()

        await broker1.close()
    })

    it('should test the new metrics endpoint', async (done) => {
        client1.subscribe({
            stream: '0xC59b3658D22e0716726819a56e164ee6825e21C2/streamr/node/metrics/sec',
        }, (res) => {
            console.log('received data??', res)
            done()
        })
    }, 10 * 1000)
})
