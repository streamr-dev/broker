module.exports = class Stream {
    constructor(id, partition, name = '') {
        this.id = id
        this.name = name
        this.partition = partition
        this.state = 'init'
        this.connections = []
    }

    addConnection(connection) {
        this.connections.push(connection)
    }

    removeConnection(connection) {
        const index = this.connections.indexOf(connection)
        if (index > -1) {
            this.connections.splice(index, 1)
        }
    }

    forEachConnection(cb) {
        this.getConnections().forEach(cb)
    }

    getConnections() {
        return this.connections
    }

    setSubscribing() {
        this.state = 'subscribing'
    }

    setSubscribed() {
        this.state = 'subscribed'
    }

    isSubscribing() {
        return this.state === 'subscribing'
    }

    isSubscribed() {
        return this.state === 'subscribed'
    }

    toString() {
        return `${this.id}::${this.partition}`
    }

    getName() {
        return this.name
    }
}