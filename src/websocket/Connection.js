const debug = require('debug')('streamr:Connection')
const qs = require('qs')
const { ErrorResponse } = require('streamr-client-protocol').ControlLayer

module.exports = class Connection {
    constructor(id, socket, request) {
        this.id = id
        this.socket = socket
        this.streams = []

        // default versions for old clients
        this.controlLayerVersion = 0
        this.messageLayerVersion = 28

        // attempt to parse versions from request parameters
        try {
            const parts = request.getQuery()
            if (parts) {
                const { controlLayerVersion, messageLayerVersion } = qs.parse(parts)
                if (controlLayerVersion && messageLayerVersion) {
                    this.controlLayerVersion = parseInt(controlLayerVersion)
                    this.messageLayerVersion = parseInt(messageLayerVersion)
                }
            }
        } catch (e) {
            console.error('failed to get request.getQuery()')
        }
    }

    addStream(stream) {
        this.streams.push(stream)
    }

    removeStream(streamId, streamPartition) {
        const i = this.streams.findIndex((s) => s.id === streamId && s.partition === streamPartition)
        if (i !== -1) {
            this.streams.splice(i, 1)
        }
    }

    forEachStream(cb) {
        this.getStreams().forEach(cb)
    }

    getStreams() {
        return this.streams.slice() // return copy
    }

    streamsAsString() {
        return this.streams.map((s) => s.toString())
    }

    send(msg) {
        const serialized = msg.serialize(this.controlLayerVersion, this.messageLayerVersion)
        debug('send: %s: %o', this.id, serialized)
        const sent = this.socket.send(serialized)
        debug('send status: ', sent)
    }

    sendError(errorMessage) {
        this.send(ErrorResponse.create(errorMessage))
    }
}

