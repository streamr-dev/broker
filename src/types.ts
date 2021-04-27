import { MetricsContext, NetworkNode } from 'streamr-network'
import { Publisher } from './Publisher'
import { StreamFetcher } from './StreamFetcher'
import { SubscriptionManager } from './SubscriptionManager'

export type Todo = any

export type EthereumAddress = string

export type PrivateKey = string

export type Url = string

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
    cassandraStorage?: Todo
    storageConfig?: Todo
}