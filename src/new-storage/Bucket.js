const createDebug = require('debug')

const isDate = (date) => new Date(date) !== 'Invalid Date'

class Bucket {
    constructor(id, streamId, partition, size, records, dateCreate, maxSize, maxRecords, keepAliveSeconds) {
        if (!id || !id.length) {
            throw new TypeError('id must be not empty string')
        }

        if (!streamId || !streamId.length) {
            throw new TypeError('streamId must be not empty string')
        }

        if (!Number.isInteger(partition) || parseInt(partition) < 0) {
            throw new TypeError('partition must be >= 0')
        }

        if (!Number.isInteger(size) || parseInt(size) < 0) {
            throw new TypeError('size must be => 0')
        }

        if (!Number.isInteger(records) || parseInt(records) < 0) {
            throw new TypeError('records must be => 0')
        }

        if (!dateCreate || !dateCreate.length || new Date(dateCreate) === 'Invalid Date') {
            throw new TypeError('dateCreate must be not valid string date')
        }

        if (!Number.isInteger(maxSize) || parseInt(maxSize) <= 0) {
            throw new TypeError('maxSize must be > 0')
        }

        if (!Number.isInteger(maxRecords) || parseInt(maxRecords) <= 0) {
            throw new TypeError('maxRecords must be > 0')
        }

        if (!Number.isInteger(keepAliveSeconds) || parseInt(keepAliveSeconds) <= 0) {
            throw new Error('keepAliveSeconds must be > 0')
        }

        this.id = id
        this.streamId = streamId
        this.partition = partition
        this.size = size
        this.records = records
        this.dateCreate = dateCreate

        this.debug = createDebug(`streamr:storage:bucket:${this.id}`)

        this._maxSize = maxSize
        this._maxRecords = maxRecords
        this._keepAliveSeconds = keepAliveSeconds

        this.ttl = new Date()
        this._updateTTL()
    }

    isFull() {
        const isFull = this.size >= this._maxSize || this.records >= this._maxRecords
        this.debug(`isFull: ${isFull} => ${this.size} >= ${this._maxSize} || ${this.records} >= ${this._maxRecords}`)
        return isFull
    }

    getId() {
        return this.id
    }

    incrementBucket(size, records = 1) {
        this.size += size
        this.records += records

        this.debug(`incremented bucket => size: ${this.size}, records: ${this.records}`)
        this._updateTTL()
    }

    _updateTTL() {
        this.ttl.setSeconds(this.ttl.getSeconds() + this._keepAliveSeconds)
        this.debug(`new ttl: ${this.ttl}`)
    }

    isAlive() {
        const isAlive = this.ttl >= new Date()
        this.debug(`isAlive: ${isAlive}, ${this.ttl} >= ${new Date()}`)
        return this.ttl >= new Date()
    }
}

module.exports = Bucket
