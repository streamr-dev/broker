const WebSocket = require('ws')
const { startTracker } = require('streamr-network')
const { wait, waitForCondition } = require('streamr-test-utils')

const { startBroker } = require('../utils')

const httpPort1 = 12341
const wsPort1 = 12351
const networkPort1 = 12361
const trackerPort = 12371

describe('ws adapter', () => {
    let tracker
    let broker

    beforeAll(async () => {
        tracker = await startTracker('127.0.0.1', trackerPort, 'tracker')
        broker = await startBroker('broker1', networkPort1, trackerPort, httpPort1, wsPort1, null, false)
    })

    afterAll(async () => {
        await broker.close()
        await tracker.stop()
    })

    const testRejection = async (connectionUrl) => {
        const ws = new WebSocket(connectionUrl)
        let gotError = false
        let closed = false
        ws.on('open', () => {
            throw new Error('Websocket should not have opened!')
        })
        ws.on('error', (err) => {
            if (err.message.includes('400')) {
                gotError = true
            } else {
                throw new Error(`Got unexpected error message: ${err.message}`)
            }
        })
        ws.on('close', () => {
            closed = true
        })
        await waitForCondition(() => gotError && closed)
    }

    it('rejects connections without preferred versions given as query parameters', async () => {
        await testRejection(`ws://127.0.0.1:${wsPort1}/api/v1/ws`)
    })

    it('rejects connections with unsupported version parameters', async () => {
        await testRejection(`ws://127.0.0.1:${wsPort1}/api/v1/ws?controlLayerVersion=666&messageLayerVersion=31`)
    })
})
