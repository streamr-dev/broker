const StreamrClient = require('streamr-client')
const mqtt = require('async-mqtt')
const fetch = require('node-fetch')
const { waitForCondition } = require('streamr-test-utils')

const createBroker = require('../src/broker')
const StorageConfig = require('../src/storage/StorageConfig')

const DEFAULT_CLIENT_OPTIONS = {
    auth: {
        apiKey: 'tester1-api-key'
    }
}

const STREAMR_URL = 'http://127.0.0.1'
const API_URL = `${STREAMR_URL}/api/v1`

function formConfig({
    name,
    networkPort,
    trackerPort,
    privateKey,
    httpPort = null,
    wsPort = null,
    mqttPort = null,
    enableCassandra = false,
    privateKeyFileName = null,
    certFileName = null,
    streamrUrl = 'http://localhost:8081/streamr-core',
    streamrAddress = '0xFCAd0B19bB29D4674531d6f115237E16AfCE377c',
    reporting = false
}) {
    const adapters = []
    if (httpPort) {
        adapters.push({
            name: 'http',
            port: httpPort,
        })
    }
    if (wsPort) {
        adapters.push({
            name: 'ws',
            port: wsPort,
            pingInterval: 3000,
            privateKeyFileName,
            certFileName
        })
    }
    if (mqttPort) {
        adapters.push({
            name: 'mqtt',
            port: mqttPort,
            streamsTimeout: 300000
        })
    }

    return {
        ethereumPrivateKey: privateKey,
        network: {
            name,
            hostname: '127.0.0.1',
            port: networkPort,
            advertisedWsUrl: null,
            isStorageNode: enableCassandra,
            trackers: [
                `ws://127.0.0.1:${trackerPort}`
            ],
            location: {
                latitude: 60.19,
                longitude: 24.95,
                country: 'Finland',
                city: 'Helsinki'
            }
        },
        cassandra: enableCassandra ? {
            hosts: ['localhost'],
            datacenter: 'datacenter1',
            username: '',
            password: '',
            keyspace: 'streamr_dev_v2',
        } : null,
        storageConfig: null,
        reporting: reporting || {
            sentry: null,
            streamr: null,
            intervalInSeconds: 10,
            perNodeMetrics: {
                enabled: false,
                wsUrl: null,
                httpUrl: null
            }
        },
        streamrUrl,
        streamrAddress,
        adapters
    }
}

function startBroker(...args) {
    return createBroker(formConfig(...args))
}

function getWsUrl(port, ssl = false, controlLayerVersion = 1, messageLayerVersion = 31) {
    return `${ssl ? 'wss' : 'ws'}://127.0.0.1:${port}/api/v1/ws?controlLayerVersion=${controlLayerVersion}&messageLayerVersion=${messageLayerVersion}`
}

function createClient(wsPort, clientOptions = DEFAULT_CLIENT_OPTIONS) {
    return new StreamrClient({
        url: getWsUrl(wsPort),
        restUrl: 'http://localhost:8081/streamr-core/api/v1',
        ...clientOptions,
    })
}

function createMqttClient(mqttPort = 9000, host = 'localhost', apiKey = 'tester1-api-key') {
    return mqtt.connect({
        hostname: host,
        port: mqttPort,
        username: '',
        password: apiKey
    })
}

class StorageAssignmentEventManager {
    constructor(wsPort, engineAndEditorAccount) {
        this.engineAndEditorAccount = engineAndEditorAccount
        this.client = createClient(wsPort, {
            auth: {
                privateKey: engineAndEditorAccount.privateKey
            }
        })
    }

    async createStream() {
        this.eventStream = await this.client.createStream({
            id: this.engineAndEditorAccount.address + StorageConfig.ASSIGNMENT_EVENT_STREAM_ID_SUFFIX
        })
    }

    async addStreamToStorageNode(streamId, storageNodeAddress, client) {
        await fetch(`${API_URL}/streams/${encodeURIComponent(streamId)}/storageNodes`, {
            body: JSON.stringify({
                address: storageNodeAddress
            }),
            headers: {
                // eslint-disable-next-line quote-props
                'Authorization': 'Bearer ' + await client.session.getSessionToken(),
                'Content-Type': 'application/json',
            },
            method: 'POST'
        })
        this.publishAddEvent(streamId)
    }

    publishAddEvent(streamId) {
        this.eventStream.publish({
            event: 'STREAM_ADDED',
            stream: {
                id: streamId,
                partitions: 1
            }
        })
    }

    close() {
        return this.client.ensureDisconnected()
    }
}

const waitForStreamPersistedInStorageNode = async (streamId, partition, nodeHost, nodeHttpPort) => {
    const isPersistent = async () => {
        const response = await fetch(`http://${nodeHost}:${nodeHttpPort}/api/v1/streams/${encodeURIComponent(streamId)}/storage/partitions/${partition}`)
        return (response.status === 200)
    }
    await waitForCondition(() => isPersistent(), undefined, 1000)
}

module.exports = {
    formConfig,
    startBroker,
    createClient,
    createMqttClient,
    getWsUrl,
    StorageAssignmentEventManager,
    waitForStreamPersistedInStorageNode
}
