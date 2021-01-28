const StreamrClient = require('streamr-client')
const ethers = require('ethers')

/*
 * Creates a stream that is writable by this node and the Core API
 */
module.exports = class EventBus {
    constructor(streamId, client) {
        this.streamId = streamId
        this.client = client
        this.subscribers = []
    }

    static async createInstance(nodeAccount) {
        const streamId = nodeAccount.address + '/eventBus'
        const client = new StreamrClient({
            auth: {
                privateKey: nodeAccount.privateKey
            },
            url: 'ws://localhost/api/v1/ws', // TODO from config
            restUrl: 'http://localhost/api/v1' // TODO from config
        })
        const instance = new EventBus(streamId, client)
        const stream = await client.getOrCreateStream({
            id: streamId
        })
        const coreApiAccount = new ethers.Wallet('0x0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF').address // TODO: private key from config file
        await stream.grantPermission('stream_get', null)
        await stream.grantPermission('stream_subscribe', null)
        await stream.grantPermission('stream_publish', coreApiAccount)
        client.subscribe(streamId, (message) => {
            console.log('DEBUG EventBus receives: ' + JSON.stringify(message))
            instance.subscribers.forEach((subscriber) => subscriber(message))
        })
        return instance
    }

    subscribe(subscriber) {
        this.subscribers.push(subscriber)
    }

    publish(message) {
        console.log('DEBUG EventBus publishes: ' + JSON.stringify(message))
        return this.client.publish(this.streamId, message)
    }

    stop() {
        return this.client.disconnect()
    }
}
