const events = require('events')

const debug = require('debug')('streamr:WebsocketServer')
const { ControlLayer } = require('streamr-client-protocol')
const LRU = require('lru-cache')
const uWS = require('uWebSockets.js')

const HttpError = require('../errors/HttpError')
const VolumeLogger = require('../VolumeLogger')
const partition = require('../partition')
const StreamStateManager = require('../StreamStateManager')

const Connection = require('./Connection')
const FieldDetector = require('./FieldDetector')

let nextId = 1

function generateId() {
    const id = `socketId-${nextId}`
    nextId += 1
    return id
}

module.exports = class WebsocketServer extends events.EventEmitter {
    constructor(
        wsPort,
        wsPath,
        networkNode,
        streamFetcher,
        publisher,
        volumeLogger = new VolumeLogger(0),
        subscriptionManager,
        partitionFn = partition,
    ) {
        super()
        this.networkNode = networkNode
        this.streamFetcher = streamFetcher
        this.publisher = publisher
        this.partitionFn = partitionFn
        this.volumeLogger = volumeLogger
        this.streams = new StreamStateManager()
        this.fieldDetector = new FieldDetector(streamFetcher)
        this.subscriptionManager = subscriptionManager
        this.streamAuthCache = new LRU({
            max: 1000,
            maxAge: 1000 * 60 * 5
        })
        this.connections = new Map()
        this.wsListenSocket = null

        this.requestHandlersByMessageType = {
            [ControlLayer.SubscribeRequest.TYPE]: this.handleSubscribeRequest,
            [ControlLayer.UnsubscribeRequest.TYPE]: this.handleUnsubscribeRequest,
            [ControlLayer.ResendRequestV0.TYPE]: this.handleResendRequestV0,
            [ControlLayer.ResendLastRequestV1.TYPE]: this.handleResendLastRequest,
            [ControlLayer.ResendFromRequestV1.TYPE]: this.handleResendFromRequest,
            [ControlLayer.ResendRangeRequestV1.TYPE]: this.handleResendRangeRequest,
            [ControlLayer.PublishRequest.TYPE]: this.handlePublishRequest,
        }

        this.networkNode.addMessageListener(this.broadcastMessage.bind(this))

        // this.wss.on('connection', this.onNewClientConnection.bind(this))
        this._updateTotalBufferSizeInterval = setInterval(() => {
            // eslint-disable-next-line max-len
            // this.volumeLogger.totalBufferSize = Object.values(this.wss.clients).reduce((totalBufferSizeSum, ws) => totalBufferSizeSum + ws.bufferedAmount, 0)
        }, 10 * 1000)

        // uWS.SSLApp({
        //     key_file_name: './misc/key.pem',
        //     cert_file_name: './misc/cert.pem',
        // })
        this.wss = uWS.App().ws(
            '/api/v1/ws',
            {
                /* Options */
                compression: 0,
                maxPayloadLength: 16 * 1024 * 1024,
                idleTimeout: 0, // don't disconnect sockets
                open: (ws, req) => {
                    if (this.wsListenSocket) {
                        // eslint-disable-next-line no-param-reassign
                        ws.id = generateId()
                        const connection = new Connection(ws.id, ws, req)
                        this.connections.set(ws.id, connection)
                        this.volumeLogger.connectionCount += 1
                        debug('onNewClientConnection: socket "%s" connected', ws.id)
                    } else {
                        req.close()
                    }
                },
                message: (ws, message, isBinary) => {
                    if (this.wsListenSocket) {
                        const connection = this.connections.get(ws.id)

                        try {
                            const data = Buffer.from(message).toString('utf-8')
                            const request = ControlLayer.ControlMessage.deserialize(data)
                            const handler = this.requestHandlersByMessageType[request.type]

                            if (handler) {
                                debug('socket "%s" sent request "%s" with contents "%o"', connection.id, request.type, request)
                                handler.call(this, connection, request)
                            } else {
                                connection.sendError(`Unknown request type: ${request.type}`)
                            }
                        } catch (err) {
                            connection.sendError(err.message || err)
                        }
                    }
                },
                drain: (ws) => {
                    console.log('WebSocket backpressure: ' + ws.getBufferedAmount())
                },
                close: (ws, code, message) => {
                    try {
                        const connection = this.connections.get(ws.id)
                        this.volumeLogger.connectionCount -= 1
                        debug('closing socket "%s" on streams "%o"', connection.id, connection.streamsAsString())

                        // Unsubscribe from all streams
                        connection.forEachStream((stream) => {
                            this.handleUnsubscribeRequest(
                                connection,
                                ControlLayer.UnsubscribeRequest.create(stream.id, stream.partition),
                                true,
                            )
                        })
                        this.connections.delete(ws.id)
                    } catch (e) {
                        console.error('Error on closing socket %s, %s', ws.id, e)
                    }
                }
            }
        ).listen(wsPort, (listenSocket) => {
            this.wsListenSocket = listenSocket
            if (listenSocket) {
                console.log(`WS adapter listening on ${wsPort}`)
                this.wsListenSocket = listenSocket
            } else {
                console.log('Failed to listen to port ' + wsPort)
                throw Error('Failed to start WebsocketServer')
            }
        })
    }

    close() {
        clearInterval(this._updateTotalBufferSizeInterval)
        this.streams.close()
        this.streamAuthCache.reset()

        return new Promise((resolve, reject) => {
            if (this.wsListenSocket) {
                uWS.us_listen_socket_close(this.wsListenSocket)
                this.wsListenSocket = null
            }
            setTimeout(() => resolve(), 1000)
        })
    }

    handlePublishRequest(connection, request) {
        const streamId = request.getStreamId()
        // TODO: should this be moved to streamr-client-protocol-js ?
        if (streamId === undefined) {
            connection.sendError('Publish request failed: Error: streamId must be defined!')
            return
        }
        // TODO: simplify with async-await
        const key = `${streamId}-${request.apiKey}-${request.sessionToken}`
        if (this.streamAuthCache.has(key)) {
            const stream = this.streamAuthCache.get(key)

            let streamPartition
            if (request.version === 0) {
                streamPartition = this.partitionFn(stream.partitions, request.partitionKey)
            }
            const streamMessage = request.getStreamMessage(streamPartition)
            this.publisher.publish(stream, streamMessage)
        } else {
            this.streamFetcher.authenticate(streamId, request.apiKey, request.sessionToken, 'write')
                .then((stream) => {
                    // TODO: should this be moved to streamr-client-protocol-js ?
                    let streamPartition
                    if (request.version === 0) {
                        streamPartition = this.partitionFn(stream.partitions, request.partitionKey)
                    }
                    const streamMessage = request.getStreamMessage(streamPartition)
                    this.fieldDetector.detectAndSetFields(stream, streamMessage, request.apiKey, request.sessionToken)
                    this.publisher.publish(stream, streamMessage)

                    this.streamAuthCache.set(key, stream)
                })
                .catch((err) => {
                    let errorMsg
                    if (err instanceof HttpError && err.code === 401) {
                        errorMsg = `You are not allowed to write to stream ${request.streamId}`
                    } else if (err instanceof HttpError && err.code === 403) {
                        errorMsg = `Authentication failed while trying to publish to stream ${request.streamId}`
                    } else if (err instanceof HttpError && err.code === 404) {
                        errorMsg = `Stream ${request.streamId} not found.`
                    } else {
                        errorMsg = `Publish request failed: ${err}`
                    }

                    connection.sendError(errorMsg)
                })
        }
    }

    // TODO: Extract resend stuff to class?
    handleResendRequest(connection, request, resendTypeHandler) {
        let nothingToResend = true

        const msgHandler = (unicastMessage) => {
            if (nothingToResend) {
                nothingToResend = false
                connection.send(ControlLayer.ResendResponseResending.create(
                    request.streamId,
                    request.streamPartition,
                    request.subId,
                ))
            }

            const { streamMessage } = unicastMessage
            this.volumeLogger.logOutput(streamMessage.getContent().length)
            connection.send(ControlLayer.UnicastMessage.create(request.subId, streamMessage))
        }

        const doneHandler = () => {
            if (nothingToResend) {
                connection.send(ControlLayer.ResendResponseNoResend.create(
                    request.streamId,
                    request.streamPartition,
                    request.subId,
                ))
            } else {
                connection.send(ControlLayer.ResendResponseResent.create(
                    request.streamId,
                    request.streamPartition,
                    request.subId,
                ))
            }
        }

        // TODO: simplify with async-await
        this.streamFetcher.authenticate(request.streamId, request.apiKey, request.sessionToken).then(() => {
            const streamingStorageData = resendTypeHandler()
            streamingStorageData.on('data', msgHandler)
            streamingStorageData.on('end', doneHandler)
        }).catch((err) => {
            connection.sendError(`Failed to request resend from stream ${
                request.streamId
            } and partition ${
                request.streamPartition
            }: ${err.message}`)
        })
    }

    handleResendLastRequest(connection, request) {
        this.handleResendRequest(connection, request, () => this.networkNode.requestResendLast(
            request.streamId,
            request.streamPartition,
            request.subId, // TODO: should generate new here or use client-provided as is?
            request.numberLast,
        ))
    }

    handleResendFromRequest(connection, request) {
        this.handleResendRequest(connection, request, () => this.networkNode.requestResendFrom(
            request.streamId,
            request.streamPartition,
            request.subId, // TODO: should generate new here or use client-provided as is?
            request.fromMsgRef.timestamp,
            request.fromMsgRef.sequenceNumber,
            request.publisherId,
            request.msgChainId,
        ))
    }

    handleResendRangeRequest(connection, request) {
        this.handleResendRequest(connection, request, () => this.networkNode.requestResendRange(
            request.streamId,
            request.streamPartition,
            request.subId, // TODO: should generate new here or use client-provided as is?
            request.fromMsgRef.timestamp,
            request.fromMsgRef.sequenceNumber,
            request.toMsgRef.timestamp,
            request.toMsgRef.sequenceNumber,
            request.publisherId,
            request.msgChainId,
        ))
    }

    // TODO: should this be moved to streamr-client-protocol-js ?
    /* eslint-disable class-methods-use-this */
    handleResendRequestV0(connection, request) {
        if (request.resendOptions.resend_last != null) {
            const requestV1 = ControlLayer.ResendLastRequest.create(
                request.streamId,
                request.streamPartition,
                request.subId,
                request.resendOptions.resend_last,
                request.sessionToken,
            )
            requestV1.apiKey = request.apiKey
            this.handleResendLastRequest(connection, requestV1)
        } else if (request.resendOptions.resend_from != null && request.resendOptions.resend_to != null) {
            const requestV1 = ControlLayer.ResendRangeRequest.create(
                request.streamId,
                request.streamPartition,
                request.subId,
                [request.resendOptions.resend_from, 0], // use offset as timestamp
                [request.resendOptions.resend_to, 0], // use offset as timestamp)
                null,
                null,
                request.sessionToken,
            )
            requestV1.apiKey = request.apiKey
            this.handleResendRangeRequest(connection, requestV1)
        } else if (request.resendOptions.resend_from != null) {
            const requestV1 = ControlLayer.ResendFromRequest.create(
                request.streamId,
                request.streamPartition,
                request.subId,
                [request.resendOptions.resend_from, 0], // use offset as timestamp
                null,
                null,
                request.sessionToken,
            )
            requestV1.apiKey = request.apiKey
            this.handleResendFromRequest(connection, requestV1)
        } else {
            debug('handleResendRequest: unknown resend request: %o', JSON.stringify(request))
            connection.sendError(`Unknown resend options: ${JSON.stringify(request.resendOptions)}`)
        }
    }

    broadcastMessage(streamMessage) {
        const streamId = streamMessage.getStreamId()
        const streamPartition = streamMessage.getStreamPartition()
        const stream = this.streams.get(streamId, streamPartition)

        if (stream) {
            stream.forEachConnection((connection) => {
                // TODO: performance fix, no need to re-create on every loop iteration
                connection.send(ControlLayer.BroadcastMessage.create(streamMessage))
            })

            this.volumeLogger.logOutput(streamMessage.getSerializedContent().length * stream.getConnections().length)
        } else {
            debug('broadcastMessage: stream "%s:%d" not found', streamId, streamPartition)
        }
    }

    handleSubscribeRequest(connection, request) {
        // TODO: simplify with async-await
        this.streamFetcher.authenticate(request.streamId, request.apiKey, request.sessionToken)
            .then((/* streamJson */) => {
                const stream = this.streams.getOrCreate(request.streamId, request.streamPartition)

                // Subscribe now if the stream is not already subscribed or subscribing
                if (!stream.isSubscribed() && !stream.isSubscribing()) {
                    stream.setSubscribing()
                    this.subscriptionManager.subscribe(request.streamId, request.streamPartition)
                    stream.setSubscribed()
                }

                stream.addConnection(connection)
                connection.addStream(stream)
                debug(
                    'handleSubscribeRequest: socket "%s" is now subscribed to streams "%o"',
                    connection.id, connection.streamsAsString()
                )
                connection.send(ControlLayer.SubscribeResponse.create(request.streamId, request.streamPartition))
            })
            .catch((response) => {
                debug(
                    'handleSubscribeRequest: socket "%s" failed to subscribe to stream %s:%d because of "%o"',
                    connection.id, request.streamId, request.streamPartition, response
                )
                connection.sendError(`Not authorized to subscribe to stream ${
                    request.streamId
                } and partition ${
                    request.streamPartition
                }`)
            })
    }

    handleUnsubscribeRequest(connection, request, noAck) {
        const stream = this.streams.get(request.streamId, request.streamPartition)

        if (stream) {
            debug('handleUnsubscribeRequest: socket "%s" unsubscribing from stream "%s:%d"', connection.id,
                request.streamId, request.streamPartition)

            stream.removeConnection(connection)
            connection.removeStream(request.streamId, request.streamPartition)

            debug(
                'handleUnsubscribeRequest: socket "%s" is still subscribed to streams "%o"',
                connection.id, connection.streamsAsString()
            )

            // Unsubscribe from stream if no connections left
            debug(
                'checkRoomEmpty: "%d" sockets remaining on stream "%s:%d"',
                stream.getConnections().length, request.streamId, request.streamPartition
            )
            if (stream.getConnections().length === 0) {
                debug(
                    'checkRoomEmpty: stream "%s:%d" is empty. Unsubscribing from NetworkNode.',
                    request.streamId, request.streamPartition
                )
                this.subscriptionManager.unsubscribe(request.streamId, request.streamPartition)
                this.streams.delete(request.streamId, request.streamPartition)
            }

            if (!noAck) {
                connection.send(ControlLayer.UnsubscribeResponse.create(request.streamId, request.streamPartition))
            }
        } else {
            debug(
                'handleUnsubscribeRequest: stream "%s:%d" no longer exists',
                request.streamId, request.streamPartition
            )
            connection.sendError(`Not subscribed to stream ${request.streamId} partition ${request.streamPartition}!`)
        }
    }
}
