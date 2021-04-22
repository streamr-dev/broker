import { EventEmitter } from 'events'
import { Logger } from 'pino'
import { Protocol } from 'streamr-network'
import { v4 as uuidv4 } from 'uuid'
import getLogger from '../helpers/logger'
import { Todo } from '../types'
import { BucketId } from './Bucket'

export type BatchId = string
export type State = string
export type DoneCallback = () => void

export class Batch extends EventEmitter {

    // TODO convert to enum and rename to uppercase
    static states = Object.freeze({
        // OPENED => LOCKED => PENDING => INSERTED
        OPENED: 'opened', // opened for adding new messages
        LOCKED: 'locked', // locked for adding new messages, because isFull or timeout
        PENDING: 'pending', // awaiting to be inserted,
        INSERTED: 'inserted'
    })

    _id: BatchId
    _bucketId: BucketId
    createdAt: number
    streamMessages: Protocol.StreamMessage[]
    size: number
    retries: number
    state: State
    doneCbs: DoneCallback[]
    logger: Logger
    _maxSize: number
    _maxRecords: number
    _maxRetries : number
    _closeTimeout: number|string
    _timeout: NodeJS.Timeout

    // TODO these "number|string" types should be just "number" (do the conversion outside constructor)
    constructor(bucketId: BucketId, maxSize: number|string, maxRecords: number|string, closeTimeout: number|string, maxRetries: number|string) {
        if (!bucketId || !bucketId.length) {
            throw new TypeError('bucketId must be not empty string')
        }

        if (!Number.isInteger(maxSize) || parseInt(maxSize as string) <= 0) {
            throw new TypeError('maxSize must be > 0')
        }

        if (!Number.isInteger(maxRecords) || parseInt(maxRecords as string) <= 0) {
            throw new TypeError('maxRecords must be > 0')
        }

        if (!Number.isInteger(closeTimeout) || parseInt(closeTimeout as string) <= 0) {
            throw new TypeError('closeTimeout must be > 0')
        }

        if (!Number.isInteger(maxRetries) || parseInt(maxRetries as string) <= 0) {
            throw new TypeError('maxRetries must be > 0')
        }

        super()

        this._id = uuidv4()
        this._bucketId = bucketId
        this.createdAt = Date.now()
        this.streamMessages = []
        this.size = 0
        this.retries = 0
        this.state = Batch.states.OPENED
        this.doneCbs = []

        this.logger = getLogger(`streamr:storage:batch:${this.getId()}`)

        // @ts-expect-error
        this._maxSize = maxSize
        // @ts-expect-error
        this._maxRecords = maxRecords
        // @ts-expect-error
        this._maxRetries = maxRetries
        this._closeTimeout = closeTimeout

        this._timeout = setTimeout(() => {
            this.logger.debug('lock timeout')
            this.lock()
        // @ts-expect-error
        }, this._closeTimeout)

        this.logger.debug('init new batch')
    }

    reachedMaxRetries() {
        return this.retries === this._maxRetries
    }

    getId() {
        return this._id
    }

    getBucketId() {
        return this._bucketId
    }

    lock() {
        clearTimeout(this._timeout)
        this._setState(Batch.states.LOCKED)
    }

    scheduleInsert() {
        clearTimeout(this._timeout)
        this.logger.debug(`scheduleRetry. retries:${this.retries}`)

        this._timeout = setTimeout(() => {
            if (this.retries < this._maxRetries) {
                this.retries += 1
            }
            this._setState(Batch.states.PENDING)
        // @ts-expect-error
        }, this._closeTimeout * this.retries)
    }

    done() {
        this.doneCbs.forEach((doneCb) => doneCb())
        this.doneCbs = []
    }

    clear() {
        this.logger.debug('cleared')
        clearTimeout(this._timeout)
        this.streamMessages = []
        this._setState(Batch.states.INSERTED)
    }

    push(streamMessage: Protocol.StreamMessage, doneCb?: DoneCallback) {
        this.streamMessages.push(streamMessage)
        this.size += Buffer.from(streamMessage.serialize()).length
        if (doneCb) {
            this.doneCbs.push(doneCb)
        }
    }

    isFull() {
        return this.size >= this._maxSize || this._getNumberOrMessages() >= this._maxRecords
    }

    _getNumberOrMessages() {
        return this.streamMessages.length
    }

    _setState(state: State) {
        this.state = state
        this.logger.debug(`emit state: ${this.state}`)
        this.emit(this.state, this.getBucketId(), this.getId(), this.state, this.size, this._getNumberOrMessages())
    }
}

