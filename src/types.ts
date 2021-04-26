import { MetricsContext, NetworkNode } from 'streamr-network'
import { Publisher } from './Publisher'
import { StreamFetcher } from './StreamFetcher'
import { SubscriptionManager } from './SubscriptionManager'
import { Storage } from './storage/Storage'

export type Todo = any

export interface StreamPart {
    id: string
    partition: number
}

export interface BrokerUtils {
    networkNode: NetworkNode
    publisher: Publisher
    streamFetcher: StreamFetcher
    metricsContext: MetricsContext
    subscriptionManager: SubscriptionManager
    cassandraStorage?: Storage
    storageConfig?: Todo
}