import fs from 'fs'
import { Server as HttpServer } from 'http'
import https, { Server as HttpsServer } from 'https'
import { AddressInfo } from 'net'
import cors from 'cors'
import express, { Request, Response } from 'express'
import getLogger from '../helpers/logger'
import { router as dataQueryEndpoints } from './DataQueryEndpoints'
import dataProduceEndpoints from './DataProduceEndpoints'
import volumeEndpoint from './VolumeEndpoint'
import dataMetadataEndpoint from './DataMetadataEndpoints'
import storageConfigEndpoints from './StorageConfigEndpoints'
import { AdapterConfig } from '../Adapter'
import { BrokerUtils } from '../types'

const logger = getLogger('streamr:httpAdapter')

export const start = (
    { port, privateKeyFileName, certFileName }: AdapterConfig, 
    { config, networkNode, publisher, streamFetcher, metricsContext, cassandraStorage, storageConfig}: BrokerUtils
) => {
    const app = express()

    // Add CORS headers
    app.use(cors())

    // Rest endpoints
    app.use('/api/v1', dataProduceEndpoints(streamFetcher, publisher))
    app.use('/api/v1', volumeEndpoint(metricsContext))

    if (config.network.isStorageNode) {
        app.use('/api/v1', dataQueryEndpoints(networkNode, streamFetcher, metricsContext))
        app.use('/api/v1', dataMetadataEndpoint(cassandraStorage))
        app.use('/api/v1', storageConfigEndpoints(storageConfig))    
    }

    app.use('*', (_req: Request, res: Response) => {
        return res.status(501).send('Unknown route')
    })

    let httpServer: HttpServer|HttpsServer
    if (privateKeyFileName && certFileName) {
        httpServer = https.createServer({
            cert: fs.readFileSync(certFileName),
            key: fs.readFileSync(privateKeyFileName)
        }, app).listen(port, () => logger.info(`HTTPS adapter listening on ${(httpServer.address() as AddressInfo).port}`))
    } else {
        httpServer = app.listen(port, () => logger.info(`HTTP adapter listening on ${(httpServer.address() as AddressInfo).port}`))
    }
    return () => new Promise((resolve, reject) => {
        httpServer.close((err?: Error) => {
            if (err) {
                reject(err)
            } else {
                // @ts-expect-error
                resolve()
            }
        })
    })
}
