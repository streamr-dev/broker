const EventEmitter = require('events')
const Stream = require('stream')

const toArray = require('stream-to-array')

module.exports = class PeriodicQuery extends EventEmitter {
    constructor(queryFunction, retryInterval, retryTimeout) {
        super()
        this.queryFunction = queryFunction
        this.retryInterval = retryInterval
        this.retryTimeout = retryTimeout
        this.readableStream = new Stream.Readable({
            objectMode: true,
            read() {},
        })
    }

    async _startFetching() {
        let results = await toArray(this.queryFunction())
        if (!results || results.length === 0) {
            this.interval = setInterval(async () => {
                results = await toArray(this.queryFunction())
                if (results && results.length > 0) {
                    this.clear()
                    this._addResultsToStream(results)
                }
            }, this.retryInterval)
            this.timeout = setTimeout(() => {
                this.clear()
                this._addResultsToStream([])
            }, this.retryTimeout)
        } else {
            this.clear()
            this._addResultsToStream(results)
        }
    }

    _addResultsToStream(results) {
        results.forEach((r) => {
            this.readableStream.push(r)
        })
        this.readableStream.push(null)
    }

    getStreamingResults() {
        this._startFetching()
        return this.readableStream
    }

    clear() {
        if (this.interval) {
            clearInterval(this.interval)
        }
        if (this.timeout) {
            clearTimeout(this.timeout)
        }
    }
}
