import { Protocol } from 'streamr-network'
import getLogger from './helpers/logger'
import { Todo } from './types'

const logger = getLogger('streamr:Stream')

export class Stream {

    id: Todo
    name: Todo
    partition: Todo
    state: Todo
    connections: Todo
    orderingUtil: Todo

    constructor(id: string, partition: number, name: string, msgHandler: Todo, gapHandler: Todo) {
        this.id = id
        this.name = name
        this.partition = partition
        this.state = 'init'
        this.connections = []
        this.orderingUtil = new Protocol.Utils.OrderingUtil(id, partition, msgHandler, (...args: Todo[]) => {
            gapHandler(id, partition, ...args)
        })
        this.orderingUtil.on('error', (err: Todo) => {
            // attach error handler in attempt to avoid uncaught exceptions
            logger.warn(err)
        })
    }

    passToOrderingUtil(streamMessage: Todo) {
        this.orderingUtil.add(streamMessage)
    }

    clearOrderingUtil() {
        this.orderingUtil.clearGaps()
    }

    addConnection(connection: Todo) {
        this.connections.push(connection)
    }

    removeConnection(connection: Todo) {
        const index = this.connections.indexOf(connection)
        if (index > -1) {
            this.connections.splice(index, 1)
        }
    }

    forEachConnection(cb: Todo) {
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

    getId() {
        return this.id
    }

    getPartition() {
        return this.partition
    }
}
