import { BrokerUtils } from './types'

export interface AdapterConfig {
    name: string
    port: number
    [x: string]: any 
}

export type AdapterStartFn = (adapterConfig: AdapterConfig, brokerUtils: BrokerUtils) => void