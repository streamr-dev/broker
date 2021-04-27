import { AdapterConfig } from './Adapter';
import { EthereumAddress, PrivateKey, Url } from './types';

export interface TrackerRegistry {
    registryAddress: EthereumAddress
    jsonRpcProvider: Url
}

export interface NetworkConfig {
    name: string,
    hostname: string,
    port: number,
    advertisedWsUrl: string | null,
    isStorageNode: boolean,
    trackers: Url[] | TrackerRegistry,
    location: {
        latitude: number,
        longitude: number,
        country: string,
        city: string
    } | null
}

export interface Config {
    ethereumPrivateKey: PrivateKey
    network: NetworkConfig,
    cassandra: {
        hosts: string[],
        username: string
        password: string
        keyspace: string,
        datacenter: string
    } | null,
    storageConfig: {
        refreshInterval: number
    } | null,
    reporting: {
        intervalInSeconds: number,
        sentry: Url | null,
        streamr: {
        streamId: string
    } | null,
        perNodeMetrics: {
        enabled: boolean
        wsUrl: Url | null
        httpUrl: Url | null
        } | null
    },
    streamrUrl: Url,
    streamrAddress: EthereumAddress,
    adapters: AdapterConfig[]
}

export interface StorageNodeConfig extends Config {
    network: NetworkConfig & {
        isStorageNode: true
    }
    cassandra: NonNullable<Config['cassandra']>
    storageConfig: NonNullable<Config['storageConfig']>
}
