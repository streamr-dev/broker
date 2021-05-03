/**
 * Endpoints for RESTful data requests
 */
import express, { Request, Response } from 'express'
import { MetricsContext, Protocol } from 'streamr-network'
import { Metrics } from 'streamr-network/dist/helpers/MetricsContext'
import { getLogger } from '../helpers/logger'
import { Todo } from '../types'
import { Storage } from '../storage/Storage'
import { authenticator } from './RequestAuthenticatorMiddleware'
import { Format, getFormat } from './DataQueryFormat'
    import { Readable, Transform } from 'stream'

const logger = getLogger('streamr:http:DataQueryEndpoints')

// TODO: move this to protocol-js
export const MIN_SEQUENCE_NUMBER_VALUE = 0
export const MAX_SEQUENCE_NUMBER_VALUE = 2147483647

class ResponseTransform extends Transform {

    format: Format
    version: number|undefined
    firstMessage = true

    constructor(format: Format, version: number|undefined) {
        super({
            writableObjectMode: true
        })
        this.format = format
        this.version = version
        this.push(format.header)
    }

    _transform(input: Protocol.MessageLayer.StreamMessage, _encoding: string, done: () => void) {
        if (this.firstMessage) {
            this.firstMessage = false
        } else {
            this.push(this.format.delimiter)
        }
        this.push(this.format.getMessageAsString(input, this.version))
        done()
    }

    _flush(done: () => void) {
        this.push(this.format.footer)
        done()
    }
}

function parseIntIfExists(x: Todo) {
    return x === undefined ? undefined : parseInt(x)
}

const sendError = (message: string, res: Response) => {
    logger.error(message)
    res.status(400).send({
        error: message
    })
}

const createEndpointRoute = (
    name: string,
    router: express.Router,
    metrics: Metrics, 
    processRequest: (req: Request, streamId: string, partition: number, onSuccess: (data: Readable) => void, onError: (msg: string) => void) => void
) => {
    router.get(`/streams/:id/data/partitions/:partition/${name}`, (req: Request, res: Response) => {
        const format = getFormat(req.query.format as string)
        if (format === undefined) {
            sendError(`Query parameter "format" is invalid: ${req.query.format}`, res)
        } else {
            metrics.record(name + 'Requests', 1)
            const streamId = req.params.id
            const partition = parseInt(req.params.partition)
            const version = parseIntIfExists(req.query.version)
            processRequest(req, streamId, partition, 
                (data: Readable) => {
                    res.writeHead(200, {
                        'Content-Type': format.contentType
                    })
                    /*
                    TODO What we should do if the data stream emits an error?
                    Previosly there was a handler:
                        data.on('error', (err: Todo) => {
                            logger.error(err)
                            res.status(500).send({
                                error: 'Failed to fetch data!'
                            })
                        })
                    which worked ok if the stream emitted an error before any 'data'
                    events. But it doesn't work if a 'data' event is emitted before
                    an 'error' event, because at that moment we have already sent
                    the HTTP status code (and content type).
                    We could e.g. call res.destroy(), but it is not possible to
                    change the HTTP status after we have already sent that.
                    Possible solutions:
                    - Allow storage to return object when a data stream is called
                      (e.g. Promise<Readable> which can reject), and assume
                      that if we get the readable, we can usually read it successfully.
                    - Analyze the success status from the first event of the stream:
                      if it is a data event, we send HTTP 200 (and content type), and
                      if it is an error event, we send HTTP 500.
                    See also skipped tests in DataQueryEndpoints.test.ts*/
                    data.pipe(new ResponseTransform(format, version)).pipe(res)
                    res.on('close', () => {
                        // stops streaming the data if the client aborts fetch
                        data.unpipe()
                    })
                },
                (errorMessage: string) => sendError(errorMessage, res)
            )
        }
    })
}

export const router = (storage: Storage, streamFetcher: Todo, metricsContext: MetricsContext) => {
    const router = express.Router()
    const metrics = metricsContext.create('broker/http')
        .addRecordedMetric('outBytes')
        .addRecordedMetric('outMessages')
        .addRecordedMetric('lastRequests')
        .addRecordedMetric('fromRequests')
        .addRecordedMetric('rangeRequests')

    router.use(
        '/streams/:id/data/partitions/:partition',
        // partition parsing middleware
        (req, res, next) => {
            if (Number.isNaN(parseInt(req.params.partition))) {
                const errMsg = `Path parameter "partition" not a number: ${req.params.partition}`
                logger.error(errMsg)
                res.status(400).send({
                    error: errMsg
                })
            } else {
                next()
            }
        },
        // authentication
        authenticator(streamFetcher, 'stream_subscribe'),
    )

    createEndpointRoute('last', router, metrics, (req: Request, streamId: string, partition: number, onSuccess: (data: Readable) => void, onError: (msg: string) => void) => {
        const count = req.query.count === undefined ? 1 : parseIntIfExists(req.query.count as string)
        if (Number.isNaN(count)) {
            onError(`Query parameter "count" not a number: ${req.query.count}`)
        } else {
            onSuccess(storage.requestLast(
                streamId,
                partition,
                count!,
            ))
        }
    })

    createEndpointRoute('from', router, metrics, (req: Request, streamId: string, partition: number, onSuccess: (data: Readable) => void, onError: (msg: string) => void) => {
        const fromTimestamp = parseIntIfExists(req.query.fromTimestamp)
        const fromSequenceNumber = parseIntIfExists(req.query.fromSequenceNumber) || MIN_SEQUENCE_NUMBER_VALUE
        const { publisherId } = req.query
        if (fromTimestamp === undefined) {
            onError('Query parameter "fromTimestamp" required.')
        } else if (Number.isNaN(fromTimestamp)) {
            onError(`Query parameter "fromTimestamp" not a number: ${req.query.fromTimestamp}`)
        } else {
            onSuccess(storage.requestFrom(
                streamId,
                partition,
                fromTimestamp,
                fromSequenceNumber,
                (publisherId as string) || null,
                null
            ))
        }
    })

    createEndpointRoute('range', router, metrics, (req: Request, streamId: string, partition: number, onSuccess: (data: Readable) => void, onError: (msg: string) => void) => {
        const fromTimestamp = parseIntIfExists(req.query.fromTimestamp)
        const toTimestamp = parseIntIfExists(req.query.toTimestamp)
        const fromSequenceNumber = parseIntIfExists(req.query.fromSequenceNumber) || MIN_SEQUENCE_NUMBER_VALUE
        const toSequenceNumber = parseIntIfExists(req.query.toSequenceNumber) || MAX_SEQUENCE_NUMBER_VALUE
        const { publisherId, msgChainId } = req.query
        if (req.query.fromOffset !== undefined || req.query.toOffset !== undefined) {
            onError('Query parameters "fromOffset" and "toOffset" are no longer supported. Please use "fromTimestamp" and "toTimestamp".')
        } else if (fromTimestamp === undefined) {
            onError('Query parameter "fromTimestamp" required.')
        } else if (Number.isNaN(fromTimestamp)) {
            onError(`Query parameter "fromTimestamp" not a number: ${req.query.fromTimestamp}`)
        } else if (toTimestamp === undefined) {
            onError('Query parameter "toTimestamp" required as well. To request all messages since a timestamp, use the endpoint /streams/:id/data/partitions/:partition/from')
        } else if (Number.isNaN(toTimestamp)) {
            onError(`Query parameter "toTimestamp" not a number: ${req.query.toTimestamp}`)
        } else if ((publisherId && !msgChainId) || (!publisherId && msgChainId)) {
            onError('Invalid combination of "publisherId" and "msgChainId"')
        } else {
            onSuccess(storage.requestRange(
                streamId,
                partition,
                fromTimestamp,
                fromSequenceNumber,
                toTimestamp,
                toSequenceNumber,
                (publisherId as string) || null,
                (msgChainId as string) || null
            ))
        }
    })

    return router
}
