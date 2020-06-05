const createDebug = require('debug')

class Bucket {
    constructor(id, streamId, partition, size, records, dateCreate, maxSize, maxRecords, keepAliveMinutes) {
        if (!id || !id.length) {
            throw new Error('id must be not empty string')
        }

        if (!streamId || !id.length) {
            throw new Error('streamId must be not empty string')
        }

        if (!Number.isInteger(partition) || parseInt(partition) < 0) {
            throw new Error('partition must be >= 0')
        }

        if (!Number.isInteger(size) || parseInt(size) < 0) {
            throw new Error('size must be => 0')
        }

        if (!Number.isInteger(records) || parseInt(records) < 0) {
            throw new Error('records must be => 0')
        }

        if (!Number.isInteger(maxSize) || parseInt(maxSize) <= 0) {
            throw new Error('maxSize must be > 0')
        }

        if (!Number.isInteger(maxRecords) || parseInt(maxRecords) <= 0) {
            throw new Error('maxRecords must be > 0')
        }

        if (!Number.isInteger(keepAliveMinutes) || parseInt(keepAliveMinutes) <= 0) {
            throw new Error('keepAliveMinutes must be > 0')
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
        this._keepAliveMinutes = keepAliveMinutes

        this.ttl = new Date()
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
        this.ttl.setMinutes(this.ttl.getMinutes() + this._keepAliveMinutes)
        this.debug(`new ttl: ${this.ttl}`)
    }

    isAlive() {
        const isAlive = new Date() > this.ttl
        this.debug(`isAlive: ${isAlive}`)
        return new Date() > this.ttl
    }
}

module.exports = Bucket
