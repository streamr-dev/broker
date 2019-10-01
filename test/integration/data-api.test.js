const assert = require('assert')

const fetch = require('node-fetch')
const WebSocket = require('@streamr/sc-uws')

const createBroker = require('../../src/broker')

const httpPort = 12345
const wsPort = 12346

describe('data-api', () => {
    let dataApi

    beforeAll(async () => {
        // Start the app
        dataApi = await createBroker({
            network: {
                id: 'broker-id',
                hostname: '127.0.0.1',
                port: '31313',
                advertisedWsUrl: 'ws://127.0.0.1:31313',
                tracker: 'ws://127.0.0.1:30300',
                isStorageNode: false
            },
            cassandra: {
                hosts: ['localhost'],
                username: '',
                password: '',
                keyspace: 'streamr_dev',
            },
            reporting: false,
            streamrUrl: 'http://localhost:8081/streamr-core',
            adapters: [
                {
                    name: 'ws',
                    port: wsPort,
                },
                {
                    name: 'http',
                    port: httpPort,
                },
            ],
        })
    })

    afterAll(() => {
        dataApi.close()
    })

    it('accepts websocket connections', (done) => {
        const ws = new WebSocket(`ws://localhost:${wsPort}/api/v1/ws`)
        ws.on('open', () => {
            ws.close()
            done()
        })
    })

    it('returns 400 response code for invalid websocket requests', (done) => {
        fetch(`http://localhost:${wsPort}/api/v1/ws`, {
            method: 'GET',
            headers: {
                Connection: 'upgrade',
                Upgrade: 'websocket',
                // Missing Sec-Websocket-Key header
            },
        }).then((res) => {
        }).catch((err) => {
            expect(err.name).toEqual('FetchError')
            expect(err.message).toEqual('request to http://localhost:12346/api/v1/ws failed, reason: socket hang up')
            done()
        })
    })
})
