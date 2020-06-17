const EventEmitter = require('events')

const createDebug = require('debug')
const { v4: uuidv4 } = require('uuid')

const STATES = Object.freeze({
    OPENED: 'batch:opened',
    CLOSED: 'batch:closed',
    PENDING: 'batch:pending'
})

class Batch extends EventEmitter {
    constructor(bucketId, maxSize, maxRecords, closeTimeout, maxRetries) {
        if (!bucketId || !bucketId.length) {
            throw new TypeError('bucketId must be not empty string')
        }

        if (!Number.isInteger(maxSize) || parseInt(maxSize) <= 0) {
            throw new TypeError('maxSize must be > 0')
        }

        if (!Number.isInteger(maxRecords) || parseInt(maxRecords) <= 0) {
            throw new TypeError('maxRecords must be > 0')
        }

        if (!Number.isInteger(closeTimeout) || parseInt(closeTimeout) <= 0) {
            throw new TypeError('closeTimeout must be > 0')
        }

        if (!Number.isInteger(maxRetries) || parseInt(maxRetries) <= 0) {
            throw new TypeError('maxRetries must be > 0')
        }

        super()

        this._id = uuidv4()
        this._bucketId = bucketId
        this.streamMessages = []
        this.size = 0
        this.retries = 0
        this.state = STATES.OPENED

        this.debug = createDebug(`streamr:storage:batch:${this.getId()}`)

        this._maxSize = maxSize
        this._maxRecords = maxRecords
        this._maxRetries = maxRetries
        this._closeTimeout = closeTimeout

        this._timeout = setTimeout(() => {
            this.debug('closing timeout')
            this.setClose()
        }, this._closeTimeout)

        this.debug('init new batch')
    }

    getId() {
        return this._id
    }

    getBucketId() {
        return this._bucketId
    }

    setClose(emitState = true) {
        clearTimeout(this._timeout)
        this.debug('closing batch')
        this._setState(STATES.CLOSED, emitState)
    }

    scheduleInsert() {
        clearTimeout(this._timeout)
        this.debug(`scheduleRetry. retries:${this.retries}`)

        this._setState(STATES.PENDING, false)
        this._timeout = setTimeout(() => {
            if (this.retries < this._maxRetries) {
                this.retries += 1
            }
            this._emitState()
        }, this._closeTimeout * this.retries)
    }

    clear() {
        this.debug('cleared')
        clearTimeout(this._timeout)
        this.streamMessages = []
    }

    push(streamMessage) {
        this.streamMessages.push(streamMessage)
        this.size += Buffer.from(streamMessage.serialize()).length
    }

    isFull() {
        return this.size >= this._maxSize || this._getNumberOrMessages() >= this._maxRecords
    }

    _getNumberOrMessages() {
        return this.streamMessages.length
    }

    _setState(state, emitState = true) {
        this.debug(`change state, current: ${this.state}, new state: ${state}`)
        this.state = state
        if (emitState) {
            this._emitState()
        }
    }

    _emitState() {
        this.emit('state', this.getBucketId(), this.getId(), this.state, this.size, this._getNumberOrMessages())
    }
}

Batch.states = STATES

module.exports = Batch
