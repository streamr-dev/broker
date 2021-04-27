import { AdapterConfig } from './Adapter';

export interface TrackerRegistry {
    registryAddress: string
    jsonRpcProvider: string
}

export interface NetworkConfig {
    name: string,
    hostname: string,
    port: number,
    advertisedWsUrl: string | null,
    isStorageNode: boolean,
    trackers: string[] | TrackerRegistry,
    location: {
        latitude: number,
        longitude: number,
        country: string,
        city: string
    } | null
}

export interface Config {
    ethereumPrivateKey: string
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
        sentry: string | null,
        streamr: {
        streamId: string
    } | null,
        perNodeMetrics: {
        enabled: boolean
        wsUrl: string | null
        httpUrl: string | null
        } | null
    },
    streamrUrl: string,
    streamrAddress: string,
    adapters: AdapterConfig[]
}

export interface StorageNodeConfig extends Config {
    network: NetworkConfig & {
        isStorageNode: true
    }
    cassandra: NonNullable<Config['cassandra']>
    storageConfig: NonNullable<Config['storageConfig']>
}
