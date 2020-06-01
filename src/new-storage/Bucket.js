const EventEmitter = require('events')

class Bucket extends EventEmitter {
    constructor(streamId, partition, maxSize, maxRecords, closeTimeout = 1000, maxRetries = 120) {
        super()

        this.streamId = streamId
        this.partition = partition
        this.records = records
        this.size = size
        this.uuid = undefined
        this.dateCreate = undefined
        this.ttl = new Date()
    }

    getUuid() {
        return this.uuid
    }

    updateUuid(uuid) {
        this.uuid = uuid
    }

    reset() {
        this.uuid = undefined
        this.records = 0
        this.size = 0
    }

    incrementBucket(records, size) {
        this.records += records
        this.size += size

        console.log(`bucket id: ${this.uuid}, size: ${this.size}, records: ${this.records}`)
    }

    updateTTL(minutes = 30) {
        this.ttl.setMinutes(this.ttl.getMinutes() + minutes)
    }
}

module.exports = Bucket
