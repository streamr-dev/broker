const EventEmitter = require('events')

class Bucket extends EventEmitter {
    constructor(id, streamId, partition, records, size, dateCreate, maxRecords, maxSize) {
        super()

        this.id = id
        this.streamId = streamId
        this.partition = partition
        this.size = size
        this.records = records
        this.dateCreate = dateCreate

        this._maxSize = maxSize
        this._maxRecords = maxRecords

        this.ttl = new Date()
    }

    isFull() {
        return this.size >= this._maxSize || this.records >= this._maxRecords
    }

    getId() {
        return this.id
    }

    incrementBucket(size, records = 1) {
        this.records += records
        this.size += size

        console.log(`incremented bucket id: ${this.id}, size: ${this.size}, records: ${this.records}`)
        this._updateTTL()
    }

    _updateTTL(minutes = 30) {
        this.ttl.setMinutes(this.ttl.getMinutes() + minutes)
    }
}

module.exports = Bucket
