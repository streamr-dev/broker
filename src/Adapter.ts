import { BrokerUtils } from './types'

export interface AdapterConfig {
    name: string
    port: number
}

export type AdapterStartFn = (adapterConfig: AdapterConfig, brokerUtils: BrokerUtils) => void