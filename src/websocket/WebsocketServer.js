const { EventEmitter } = require('events')

const { v4: uuidv4 } = require('uuid')
const qs = require('qs')
const { ControlLayer, MessageLayer, Errors, Utils } = require('streamr-network').Protocol
const ab2str = require('arraybuffer-to-string')
const uWS = require('uWebSockets.js')

const logger = require('../helpers/logger')('streamr:WebsocketServer')
const HttpError = require('../errors/HttpError')
const FailedToPublishError = require('../errors/FailedToPublishError')
const StreamStateManager = require('../StreamStateManager')

const Connection = require('./Connection')

module.exports = class WebsocketServer extends EventEmitter {
    constructor(
        wss,
        port,
        networkNode,
        streamFetcher,
        publisher,
        metricsContext,
        subscriptionManager,
        pingInterval = 60 * 1000,
    ) {
        super()
        this.wss = wss
        this._listenSocket = null
        this.networkNode = networkNode
        this.streamFetcher = streamFetcher
        this.publisher = publisher
        this.connections = new Map()
        this.streams = new StreamStateManager(
            this._broadcastMessage.bind(this),
            (streamId, streamPartition, from, to, publisherId, msgChainId) => {
                this.networkNode.requestResendRange(
                    streamId,
                    streamPartition,
                    uuidv4(),
                    from.timestamp,
                    from.sequenceNumber,
                    to.timestamp,
                    to.sequenceNumber,
                    publisherId,
                    msgChainId
                ).on('data', (unicastMessage) => {
                    this._handleStreamMessage(unicastMessage.streamMessage)
                })
            }
        )
        this.pingInterval = pingInterval
        this.subscriptionManager = subscriptionManager
        this.metrics = metricsContext.create('broker/ws')
            .addRecordedMetric('outBytes')
            .addRecordedMetric('outMessages')
            .addQueriedMetric('connections', () => this.connections.size)
            .addQueriedMetric('totalWebSocketBuffer', () => {
                let totalBufferSize = 0
                this.connections.forEach((connection, id) => {
                    if (connection.socket) {
                        totalBufferSize += connection.socket.getBufferedAmount()
                    }
                })
                return totalBufferSize
            })
            .addQueriedMetric('clientVersions', () => {
                const control = {}
                const message = {}
                const pairs = {}
                this.connections.forEach((connection, id) => {
                    const { controlLayerVersion, messageLayerVersion } = connection
                    const pairKey = controlLayerVersion + '->' + messageLayerVersion
                    if (control[controlLayerVersion] == null) {
                        control[controlLayerVersion] = 0
                    }
                    if (message[messageLayerVersion] == null) {
                        message[messageLayerVersion] = 0
                    }
                    if (pairs[pairKey] == null) {
                        pairs[pairKey] = 0
                    }
                    control[controlLayerVersion] += 1
                    message[messageLayerVersion] += 1
                    pairs[pairKey] += 1
                })
                return {
                    control,
                    message,
                    pairs
                }
            })

        this.requestHandlersByMessageType = {
            [ControlLayer.ControlMessage.TYPES.SubscribeRequest]: this.handleSubscribeRequest,
            [ControlLayer.ControlMessage.TYPES.UnsubscribeRequest]: this.handleUnsubscribeRequest,
            [ControlLayer.ControlMessage.TYPES.ResendLastRequest]: this.handleResendLastRequest,
            [ControlLayer.ControlMessage.TYPES.ResendFromRequest]: this.handleResendFromRequest,
            [ControlLayer.ControlMessage.TYPES.ResendRangeRequest]: this.handleResendRangeRequest,
            [ControlLayer.ControlMessage.TYPES.PublishRequest]: this.handlePublishRequest,
        }

        this.networkNode.addMessageListener(this._handleStreamMessage.bind(this))

        this._pingInterval = setInterval(() => {
            this._pingConnections()
        }, this.pingInterval)

        this.wss.listen(port, (token) => {
            if (token) {
                this._listenSocket = token
                logger.info('WS adapter listening on ' + port)
            } else {
                logger.info('Failed to listen to port ' + port)
                this.close()
            }
        })

        this.wss.ws('/api/v1/ws', {
            /* Options */
            compression: 0,
            maxPayloadLength: 1024 * 1024,
            maxBackpressure: Connection.HIGH_BACK_PRESSURE + (1024 * 1024), // add 1MB safety margin
            idleTimeout: 3600, // 1 hour
            upgrade: (res, req, context) => {
                let controlLayerVersion
                let messageLayerVersion

                // parse protocol version instructions from query parameters
                if (req.getQuery()) {
                    const query = qs.parse(req.getQuery())
                    if (query.controlLayerVersion && query.messageLayerVersion) {
                        controlLayerVersion = parseInt(query.controlLayerVersion)
                        messageLayerVersion = parseInt(query.messageLayerVersion)
                    }
                }

                try {
                    WebsocketServer.validateProtocolVersions(controlLayerVersion, messageLayerVersion)
                } catch (err) {
                    logger.debug('Rejecting connection with status 400 due to: %s, query params: %s', err.message, req.getQuery())
                    logger.debug(err)
                    res.writeStatus('400')
                    res.write(err.message)
                    res.end()
                    return
                }

                /* This immediately calls open handler, you must not use res after this call */
                res.upgrade(
                    {
                        controlLayerVersion,
                        messageLayerVersion,
                    },
                    /* Spell these correctly */
                    req.getHeader('sec-websocket-key'),
                    req.getHeader('sec-websocket-protocol'),
                    req.getHeader('sec-websocket-extensions'),
                    context
                )
            },
            open: (ws) => {
                const connection = new Connection(ws, ws.controlLayerVersion, ws.messageLayerVersion)
                this.connections.set(connection.id, connection)
                logger.debug('onNewClientConnection: socket "%s" connected', connection.id)
                // eslint-disable-next-line no-param-reassign
                ws.connectionId = connection.id

                connection.on('forceClose', (err) => {
                    try {
                        connection.socket.close()
                    } catch (e) {
                        // no need to check this error
                    } finally {
                        logger.warn('forceClose connection with id %s, because of %s', connection.id, err)
                        this._removeConnection(connection)
                    }
                })
            },
            message: (ws, message, isBinary) => {
                const connection = this.connections.get(ws.connectionId)

                if (connection) {
                    const copy = (src) => {
                        const dst = new ArrayBuffer(src.byteLength)
                        new Uint8Array(dst).set(new Uint8Array(src))
                        return dst
                    }

                    const msg = copy(message)

                    setImmediate(() => {
                        if (connection.isDead()) {
                            return
                        }

                        let request
                        try {
                            request = ControlLayer.ControlMessage.deserialize(ab2str(msg), false)
                        } catch (err) {
                            connection.send(new ControlLayer.ErrorResponse({
                                requestId: '', // Can't echo the requestId of the request since parsing the request failed
                                errorMessage: err.message || err,
                                errorCode: 'INVALID_REQUEST',
                            }))
                            return
                        }

                        try {
                            const handler = this.requestHandlersByMessageType[request.type]
                            if (handler) {
                                logger.debug('socket "%s" sent request "%s" with contents "%o"', connection.id, request.type, request)
                                handler.call(this, connection, request)
                            } else {
                                connection.send(new ControlLayer.ErrorResponse({
                                    version: request.version,
                                    requestId: request.requestId,
                                    errorMessage: `Unknown request type: ${request.type}`,
                                    errorCode: 'INVALID_REQUEST',
                                }))
                            }
                        } catch (err) {
                            connection.send(new ControlLayer.ErrorResponse({
                                version: request.version,
                                requestId: request.requestId,
                                errorMessage: err.message || err,
                                errorCode: err.errorCode || 'ERROR_WHILE_HANDLING_REQUEST',
                            }))
                        }
                    })
                }
            },
            drain: (ws) => {
                const connection = this.connections.get(ws.connectionId)
                if (connection) {
                    connection.evaluateBackPressure()
                }
            },
            close: (ws, code, message) => {
                const connection = this.connections.get(ws.connectionId)

                if (connection) {
                    logger.debug('closing socket "%s" on streams "%o"', connection.id, connection.streamsAsString())
                    this._removeConnection(connection)
                }
            },
            pong: (ws) => {
                const connection = this.connections.get(ws.connectionId)

                if (connection) {
                    logger.debug(`received from ${connection.id} "pong" frame`)
                    connection.respondedPong = true
                }
            }
        })
    }

    static validateProtocolVersions(controlLayerVersion, messageLayerVersion) {
        if (controlLayerVersion === undefined || messageLayerVersion === undefined) {
            throw new Error('Missing version negotiation! Must give controlLayerVersion and messageLayerVersion as query parameters!')
        }

        // Validate that the requested versions are supported
        if (ControlLayer.ControlMessage.getSupportedVersions().indexOf(controlLayerVersion) < 0) {
            throw new Errors.UnsupportedVersionError(controlLayerVersion, `Supported ControlLayer versions: ${
                JSON.stringify(ControlLayer.ControlMessage.getSupportedVersions())
            }. Are you using an outdated library?`)
        }

        if (MessageLayer.StreamMessage.getSupportedVersions().indexOf(messageLayerVersion) < 0) {
            throw new Errors.UnsupportedVersionError(messageLayerVersion, `Supported MessageLayer versions: ${
                JSON.stringify(MessageLayer.StreamMessage.getSupportedVersions())
            }. Are you using an outdated library?`)
        }
    }

    _removeConnection(connection) {
        this.connections.delete(connection.id)

        // Unsubscribe from all streams
        connection.forEachStream((stream) => {
            // for cleanup, spoof an UnsubscribeRequest to ourselves on the removed connection
            this.handleUnsubscribeRequest(
                connection,
                new ControlLayer.UnsubscribeRequest({
                    requestId: uuidv4(),
                    streamId: stream.id,
                    streamPartition: stream.partition,
                }),
                true,
            )
        })

        // Cancel all resends
        connection.getOngoingResends().forEach((resend) => {
            resend.destroy()
        })

        connection.markAsDead()
    }

    close() {
        clearInterval(this._pingInterval)

        this.streams.close()

        return new Promise((resolve, reject) => {
            try {
                this.connections.forEach((connection) => connection.socket.close())
            } catch (e) {
                // ignoring any error
            }

            if (this._listenSocket) {
                uWS.us_listen_socket_close(this._listenSocket)
                this._listenSocket = null
            }

            setTimeout(() => resolve(), 100)
        })
    }

    async handlePublishRequest(connection, request) {
        const { streamMessage } = request

        try {
            // Legacy validation: for unsigned messages, we additionally need to do an authenticated check of publish permission
            // This can be removed when support for unsigned messages is dropped!
            if (!streamMessage.signature) {
                // checkPermission is cached
                await this.streamFetcher.checkPermission(request.streamMessage.getStreamId(), request.apiKey, request.sessionToken, 'stream_publish')
            }

            await this.publisher.validateAndPublish(streamMessage)
        } catch (err) {
            let errorMessage
            let errorCode
            if (err instanceof HttpError && err.code === 401) {
                errorMessage = `Authentication failed while trying to publish to stream ${streamMessage.getStreamId()}`
                errorCode = 'AUTHENTICATION_FAILED'
            } else if (err instanceof HttpError && err.code === 403) {
                errorMessage = `You are not allowed to write to stream ${streamMessage.getStreamId()}`
                errorCode = 'PERMISSION_DENIED'
            } else if (err instanceof HttpError && err.code === 404) {
                errorMessage = `Stream ${streamMessage.getStreamId()} not found.`
                errorCode = 'NOT_FOUND'
            } else if (err instanceof FailedToPublishError) {
                errorMessage = err.message
                errorCode = 'FUTURE_TIMESTAMP'
            } else {
                errorMessage = `Publish request failed: ${err.message || err}`
                errorCode = 'REQUEST_FAILED'
            }

            connection.send(new ControlLayer.ErrorResponse({
                version: request.version,
                requestId: request.requestId,
                errorMessage,
                errorCode,
            }))
        }
    }

    // TODO: Extract resend stuff to class?
    async handleResendRequest(connection, request, resendTypeHandler) {
        let nothingToResend = true
        let sentMessages = 0

        const msgHandler = (unicastMessage) => {
            if (nothingToResend) {
                nothingToResend = false
                connection.send(new ControlLayer.ResendResponseResending(request))
            }

            const { streamMessage } = unicastMessage
            this.metrics.record('outBytes', streamMessage.getSerializedContent().length)
            this.metrics.record('outMessages', 1)
            sentMessages += 1
            connection.send(new ControlLayer.UnicastMessage({
                version: request.version,
                requestId: request.requestId,
                streamMessage,
            }))
        }

        const doneHandler = () => {
            logger.info('Finished resend %s for stream %s with a total of %d sent messages', request.requestId, request.streamId, sentMessages)
            if (nothingToResend) {
                connection.send(new ControlLayer.ResendResponseNoResend({
                    version: request.version,
                    requestId: request.requestId,
                    streamId: request.streamId,
                    streamPartition: request.streamPartition,
                }))
            } else {
                connection.send(new ControlLayer.ResendResponseResent({
                    version: request.version,
                    requestId: request.requestId,
                    streamId: request.streamId,
                    streamPartition: request.streamPartition,
                }))
            }
        }

        try {
            await this._validateSubscribeOrResendRequest(request)
            if (connection.isDead()) {
                return
            }
            const streamingStorageData = resendTypeHandler()
            const pauseHandler = () => streamingStorageData.pause()
            const resumeHandler = () => streamingStorageData.resume()
            connection.addOngoingResend(streamingStorageData)
            streamingStorageData.on('data', msgHandler)
            streamingStorageData.on('end', doneHandler)
            connection.on('highBackPressure', pauseHandler)
            connection.on('lowBackPressure', resumeHandler)
            streamingStorageData.once('end', () => {
                connection.removeOngoingResend(streamingStorageData)
                connection.removeListener('highBackPressure', pauseHandler)
                connection.removeListener('lowBackPressure', resumeHandler)
            })
        } catch (err) {
            connection.send(new ControlLayer.ErrorResponse({
                version: request.version,
                requestId: request.requestId,
                errorMessage: `Failed to request resend from stream ${request.streamId} and partition ${request.streamPartition}: ${err.message}`,
                errorCode: err.errorCode || 'RESEND_FAILED',
            }))
        }
    }

    async handleResendLastRequest(connection, request) {
        await this.handleResendRequest(connection, request, () => this.networkNode.requestResendLast(
            request.streamId,
            request.streamPartition,
            uuidv4(),
            request.numberLast,
        ))
    }

    async handleResendFromRequest(connection, request) {
        await this.handleResendRequest(connection, request, () => this.networkNode.requestResendFrom(
            request.streamId,
            request.streamPartition,
            uuidv4(),
            request.fromMsgRef.timestamp,
            request.fromMsgRef.sequenceNumber,
            request.publisherId,
            request.msgChainId,
        ))
    }

    async handleResendRangeRequest(connection, request) {
        await this.handleResendRequest(connection, request, () => this.networkNode.requestResendRange(
            request.streamId,
            request.streamPartition,
            uuidv4(),
            request.fromMsgRef.timestamp,
            request.fromMsgRef.sequenceNumber,
            request.toMsgRef.timestamp,
            request.toMsgRef.sequenceNumber,
            request.publisherId,
            request.msgChainId,
        ))
    }

    _broadcastMessage(streamMessage) {
        const streamId = streamMessage.getStreamId()
        const streamPartition = streamMessage.getStreamPartition()
        const stream = this.streams.get(streamId, streamPartition)

        if (stream) {
            stream.forEachConnection((connection) => {
                connection.send(new ControlLayer.BroadcastMessage({
                    requestId: '', // TODO: can we have here the requestId of the original SubscribeRequest?
                    streamMessage,
                }))
            })

            this.metrics.record('outBytes', streamMessage.getSerializedContent().length * stream.getConnections().length)
            this.metrics.record('outMessages', stream.getConnections().length)
        } else {
            logger.debug('broadcastMessage: stream "%s:%d" not found', streamId, streamPartition)
        }
    }

    _pingConnections() {
        const connections = [...this.connections.values()]
        connections.forEach((connection) => {
            try {
                // didn't get "pong" in pingInterval
                if (connection.respondedPong !== undefined && !connection.respondedPong) {
                    throw Error('Connection is not active')
                }

                // eslint-disable-next-line no-param-reassign
                connection.respondedPong = false
                connection.ping()
                logger.debug(`pinging ${connection.id}`)
            } catch (e) {
                logger.error(`Failed to ping connection: ${connection.id}, error ${e}`)
                connection.emit('forceClose')
            }
        })
    }

    _handleStreamMessage(streamMessage) {
        const streamId = streamMessage.getStreamId()
        const streamPartition = streamMessage.getStreamPartition()
        const stream = this.streams.get(streamId, streamPartition)
        if (stream) {
            setImmediate(() => stream.passToOrderingUtil(streamMessage), 0)
        } else {
            logger.debug('_handleStreamMessage: stream "%s:%d" not found', streamId, streamPartition)
        }
    }

    async _validateSubscribeOrResendRequest(request) {
        if (Utils.StreamMessageValidator.isKeyExchangeStream(request.streamId)) {
            if (request.streamPartition !== 0) {
                throw new Error(`Key exchange streams only have partition 0. Tried to subscribe to ${request.streamId}:${request.streamPartition}`)
            }
        } else {
            await this.streamFetcher.checkPermission(request.streamId, request.apiKey, request.sessionToken, 'stream_subscribe')
        }
    }

    async handleSubscribeRequest(connection, request) {
        try {
            await this._validateSubscribeOrResendRequest(request)

            if (connection.isDead()) {
                return
            }
            const stream = this.streams.getOrCreate(request.streamId, request.streamPartition)

            // Subscribe now if the stream is not already subscribed or subscribing
            if (!stream.isSubscribed() && !stream.isSubscribing()) {
                stream.setSubscribing()
                this.subscriptionManager.subscribe(request.streamId, request.streamPartition)
                stream.setSubscribed()
            }

            stream.addConnection(connection)
            connection.addStream(stream)
            logger.debug(
                'handleSubscribeRequest: socket "%s" is now subscribed to streams "%o"',
                connection.id, connection.streamsAsString()
            )
            connection.send(new ControlLayer.SubscribeResponse({
                version: request.version,
                requestId: request.requestId,
                streamId: request.streamId,
                streamPartition: request.streamPartition,
            }))
        } catch (err) {
            logger.debug(
                'handleSubscribeRequest: socket "%s" failed to subscribe to stream %s:%d because of "%o"',
                connection.id, request.streamId, request.streamPartition, err
            )

            let errorMessage
            let errorCode
            if (err instanceof HttpError && err.code === 401) {
                errorMessage = `Authentication failed while trying to subscribe to stream ${request.streamId}`
                errorCode = 'AUTHENTICATION_FAILED'
            } else if (err instanceof HttpError && err.code === 403) {
                errorMessage = `You are not allowed to subscribe to stream ${request.streamId}`
                errorCode = 'PERMISSION_DENIED'
            } else if (err instanceof HttpError && err.code === 404) {
                errorMessage = `Stream ${request.streamId} not found.`
                errorCode = 'NOT_FOUND'
            } else {
                errorMessage = `Subscribe request failed: ${err}`
                errorCode = 'REQUEST_FAILED'
            }

            connection.send(new ControlLayer.ErrorResponse({
                version: request.version,
                requestId: request.requestId,
                errorMessage,
                errorCode,
            }))
        }
    }

    handleUnsubscribeRequest(connection, request, noAck = false) {
        const stream = this.streams.get(request.streamId, request.streamPartition)

        if (stream) {
            logger.debug('handleUnsubscribeRequest: socket "%s" unsubscribing from stream "%s:%d"', connection.id,
                request.streamId, request.streamPartition)

            stream.removeConnection(connection)
            connection.removeStream(request.streamId, request.streamPartition)

            logger.debug(
                'handleUnsubscribeRequest: socket "%s" is still subscribed to streams "%o"',
                connection.id, connection.streamsAsString()
            )

            // Unsubscribe from stream if no connections left
            logger.debug(
                'checkRoomEmpty: "%d" sockets remaining on stream "%s:%d"',
                stream.getConnections().length, request.streamId, request.streamPartition
            )
            if (stream.getConnections().length === 0) {
                logger.debug(
                    'checkRoomEmpty: stream "%s:%d" is empty. Unsubscribing from NetworkNode.',
                    request.streamId, request.streamPartition
                )
                this.subscriptionManager.unsubscribe(request.streamId, request.streamPartition)
                this.streams.delete(request.streamId, request.streamPartition)
            }

            if (!noAck) {
                connection.send(new ControlLayer.UnsubscribeResponse({
                    version: request.version,
                    requestId: request.requestId,
                    streamId: request.streamId,
                    streamPartition: request.streamPartition
                }))
            }
        } else {
            logger.debug(
                'handleUnsubscribeRequest: stream "%s:%d" no longer exists',
                request.streamId, request.streamPartition
            )
            if (!noAck) {
                connection.send(new ControlLayer.ErrorResponse({
                    version: request.version,
                    requestId: request.requestId,
                    errorMessage: `Not subscribed to stream ${request.streamId} partition ${request.streamPartition}!`,
                    errorCode: 'INVALID_REQUEST',
                }))
            }
        }
    }
}
