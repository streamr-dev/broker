import fetch from 'node-fetch'
import { MetricsContext } from 'streamr-network'
import { start } from '../../../src/http/index'
import { Publisher } from '../../../src/Publisher'
import { StreamFetcher } from '../../../src/StreamFetcher'
import { STREAMR_DOCKER_DEV_HOST } from '../../utils'

const PORT = 12345
const ROOT_URL = `http://${STREAMR_DOCKER_DEV_HOST}:${PORT}/api/v1`

describe('HTTP adapter', () => {

    test('not a storage node', async () => {
        const closeAdapter = start({
            port: PORT
        }, {
            config: {
                network: {
                    isStorageNode: false
                }
            },
            streamFetcher: new StreamFetcher(''),
            publisher: new Publisher({} as any, {}, new MetricsContext('')),
            metricsContext: new MetricsContext('')
        } as any)
        const streamId = 'mock-stream-id'
        const streamPartition = 0
        const paths = [
            `/streams/${streamId}/storage/partitions/${streamPartition}`,
            `/streams/${streamId}/metadata/partitions/${streamPartition}`,
            '/foobar'
        ]
        for await (const path of paths) {
            const url = `${ROOT_URL}${path}`
            const response = await fetch(url)
            expect(response.status).toBe(501)    
        }
        await closeAdapter()
    })

})
