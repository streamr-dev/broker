/**
 * Instead of inserting StreamMessages into Cassandra one at a time as they
 * arrive, MicroBatchingStrategy collects several StreamMessages (of same
 * streamId and streamPartition) together for batched inserts. This tends to
 * lead to better throughput.
 *
 * Based on earlier work in CassandraBatchReporter.java in cloud-broker
 * project.
 *
 * Background: https://dzone.com/articles/efficient-cassandra-write
 */

class SharedContext {
    constructor(insertFn, baseCommitIntervalInMs, maxFailMultiplier, doNotGrowBatchAfterBytes, logErrors) {
        this.insertFn = insertFn
        this.baseCommitIntervalInMs = baseCommitIntervalInMs
        this.maxFailMultiplier = maxFailMultiplier
        this.doNotGrowBatchAfterBytes = doNotGrowBatchAfterBytes
        this.logErrors = logErrors
        this.failMultiplier = 1
    }

    async insert(streamMessages) {
        try {
            await this.insertFn(streamMessages)
            this._resetFailMultiplier()
        } catch (e) {
            if (this.logErrors) {
                console.error(e)
            }
            this._growFailMultiplier()
            throw e
        }
    }

    getCommitIntervalInMs() {
        return this.baseCommitIntervalInMs * this.failMultiplier
    }

    _resetFailMultiplier() {
        this.failMultiplier = 1
    }

    _growFailMultiplier() {
        const candidate = this.failMultiplier * 2
        if (candidate <= this.maxFailMultiplier) {
            this.failMultiplier = candidate
        }
    }
}

class Batch {
    constructor(sharedContext) {
        this.streamMessages = []
        this.totalSize = 0
        this.donePromise = new Promise((resolve, reject) => {
            this.resolve = resolve
            this.reject = reject
        })
        this.timeoutRef = null
        this.sharedContext = sharedContext
        this._scheduleInsert()
    }

    push(streamMessage) {
        this.streamMessages.push(streamMessage)
        this.totalSize += streamMessage.getContent().length
        return this.donePromise
    }

    isFull() {
        return this.totalSize >= this.sharedContext.doNotGrowBatchAfterBytes
    }

    cancel() {
        clearTimeout(this.timeoutRef)
        this.reject()
    }

    _scheduleInsert() {
        this.timeoutRef = setTimeout(() => this._tryInsert(), this.sharedContext.getCommitIntervalInMs())
    }

    async _tryInsert() {
        try {
            await this.sharedContext.insert(this.streamMessages)
            this.resolve()
        } catch (e) {
            this._scheduleInsert()
        }
    }
}

class MicroBatchingStrategy {
    constructor({
        insertFn,
        baseCommitIntervalInMs = 1000,
        maxFailMultiplier = 64,
        doNotGrowBatchAfterBytes = 1024 * 1024 * 2,
        logErrors = true
    }) {
        this.batches = {} // streamId-streamPartition => Batch
        this.sharedContext = new SharedContext(
            insertFn,
            baseCommitIntervalInMs,
            maxFailMultiplier,
            doNotGrowBatchAfterBytes,
            logErrors
        )
    }

    store(streamMessage) {
        const key = `${streamMessage.getStreamId()}::${streamMessage.getStreamPartition()}`

        if (this.batches[key] === undefined || this.batches[key].isFull()) {
            const newBatch = new Batch(this.sharedContext)
            newBatch.donePromise.finally(() => this._cleanUpIfStale(key, newBatch))
            this.batches[key] = newBatch
        }

        return this.batches[key].push(streamMessage)
    }

    close() {
        Object.values(this.batches).forEach((batch) => batch.cancel())
        this.batches = {}
    }

    /**
     * If a batch with key `key` was successfully inserted into Cassandra but
     * not enough messages were pushed meanwhile with same `key` for a new
     * batch to emerge, the batch in `this.batches[key]` will still refer to
     * the stale, inserted batch. Clean it up to save memory.
     */
    _cleanUpIfStale(key, batch) {
        if (Object.is(this.batches[key], batch)) {
            delete this.batches[key]
        }
    }
}

module.exports = MicroBatchingStrategy
