import { NetworkNode } from 'streamr-network'
import { Todo } from './types'

export class SubscriptionManager {
    publishSessionTimeout: number
    networkNode: NetworkNode
    streams: Map<Todo,Todo>
    publishTimeouts: Map<string, NodeJS.Timeout>

    constructor(networkNode: NetworkNode, publishSessionTimeout = 30 * 1000) {
        this.networkNode = networkNode
        this.publishSessionTimeout = publishSessionTimeout
        this.streams = new Map()
        this.publishTimeouts = new Map()
    }

    recordPublish(streamId: string, streamPartition = 0): void {
        this.networkNode.subscribe(streamId, streamPartition)

        const key = `${streamId}::${streamPartition}`
        clearTimeout(this.publishTimeouts.get(key)!)
        this.publishTimeouts.set(key, setTimeout(() => {
            this.publishTimeouts.delete(key)
            if (!this.streams.has(key)) {
                this.networkNode.unsubscribe(streamId, streamPartition)
            }
        }, this.publishSessionTimeout))
    }

    subscribe(streamId: string, streamPartition = 0): void {
        const key = `${streamId}::${streamPartition}`
        this.streams.set(key, (this.streams.get(key) || 0) + 1)

        this.networkNode.subscribe(streamId, streamPartition)
    }

    unsubscribe(streamId: string, streamPartition = 0): void {
        const key = `${streamId}::${streamPartition}`
        if (this.streams.has(key)) {
            if (this.streams.get(key) <= 1) {
                this.streams.delete(key)
                if (!this.publishTimeouts.has(key)) {
                    this.networkNode.unsubscribe(streamId, streamPartition)
                }
            } else {
                this.streams.set(key, this.streams.get(key) - 1)
            }
        }
    }

    clear(): void {
        this.publishTimeouts.forEach((timeout) => clearTimeout(timeout))
        this.publishTimeouts = new Map()
        this.streams = new Map()
    }
}
