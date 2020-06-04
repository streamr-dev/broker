const EventEmitter = require('events')

const createDebug = require('debug')
const { v4: uuidv4 } = require('uuid')

const STATES = Object.freeze({
    OPENED: 'batch:opened',
    CLOSED: 'batch:closed',
    PENDING: 'batch:pending',
    INSERTED: 'batch:inserted'
})

class Batch extends EventEmitter {
    constructor(bucketId, maxSize, maxRecords, closeTimeout = 1000, maxRetries = 120) {
        super()

        this.id = uuidv4()
        this.bucketId = bucketId
        this.streamMessages = []
        this.size = 0
        this.retries = 0
        this.state = STATES.OPENED

        this.debug = createDebug(`streamr:storage:batch:${this.id}`)

        this._maxSize = maxSize
        this._maxRecords = maxRecords
        this._maxRetries = maxRetries
        this._closeTimeout = closeTimeout

        this._timeout = setTimeout(() => this.close(), closeTimeout)
    }

    setPending() {
        clearTimeout(this._timeout)
        this._setState(STATES.PENDING)
    }

    scheduleRetry() {
        this.debug(`scheduleRetry. retries:${this.retries}`)

        if (this.retries < this._maxRetries) {
            this.retries += 1
        }

        clearTimeout(this._timeout)
        this._timeout = setTimeout(() => this._emitState(), this._closeTimeout * this.retries)
    }

    clear() {
        clearTimeout(this._timeout)
        this.streamMessages = []
    }

    push(streamMessage) {
        this.streamMessages.push(streamMessage)
        this.size += Buffer.from(streamMessage.serialize()).length
    }

    close() {
        this.debug('closing batch by timeout')
        this._setState(STATES.CLOSED)
    }

    isFull() {
        const isFull = this.size >= this._maxSize || this._getNumberOrMessages() >= this._maxRecords
        this.debug(`isFull: ${isFull}`)
        return isFull
    }

    _getNumberOrMessages() {
        return this.streamMessages.length
    }

    _setState(state) {
        this.debug(`change state, current: ${this.state}, new state: ${state}`)
        this.state = state
        this._emitState()
    }

    _emitState() {
        this.emit('state', this.id, this.state, this.size, this._getNumberOrMessages())
    }
}

Batch.states = STATES

module.exports = Batch
