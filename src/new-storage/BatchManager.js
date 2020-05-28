const EventEmitter = require('events')

const Batch = require('./Batch')

const INSERT_STATEMENT = 'INSERT INTO stream_data '
    + '(id, partition, ts, sequence_no, publisher_id, msg_chain_id, payload) '
    + 'VALUES (?, ?, ?, ?, ?, ?, ?)'

const INSERT_STATEMENT_WITH_TTL = 'INSERT INTO stream_data '
    + '(id, partition, ts, sequence_no, publisher_id, msg_chain_id, payload) '
    + 'VALUES (?, ?, ?, ?, ?, ?, ?) USING TTL 259200' // 3 days

class BatchManager extends EventEmitter {
    constructor(cassandraClient, useTtl = false, logErrors = true) {
        super()

        this.batches = {}
        this.pendingBatches = {}
        this.cassandraClient = cassandraClient
        this.insertStatement = useTtl ? INSERT_STATEMENT_WITH_TTL : INSERT_STATEMENT
        this.logErrors = logErrors
    }

    store(streamMessage) {
        const key = `${streamMessage.getStreamId()}::${streamMessage.getStreamPartition()}`
        const batch = this.batches[key]

        if (batch && batch.isFull()) {
            batch.close()

            this.pendingBatches[batch.id] = batch
            this.pendingBatches[batch.id].setPending()

            this.batches[key] = undefined
        }

        if (this.batches[key] === undefined) {
            this.batches[key] = new Batch(10000, 100, 30000)
            this.batches[key].on('state', (batchId, batchState, batchSize, batchNumberOfRecords) => this._batchChangedState(batchId, batchState, batchSize, batchNumberOfRecords))
        }

        this.batches[key].push(streamMessage)
    }

    _batchChangedState(batchId, batchState, batchSize, batchNumberOfRecords) {
        console.log(`batchId: ${batchId}, batchState: ${batchState}, batchSize: ${batchSize}, batchNumberOfRecords: ${batchNumberOfRecords}`)
        if (batchState === Batch.states.PENDING) {
            this._insert(batchId)
        }
    }

    async _insert(batchId) {
        const batch = this.pendingBatches[batchId]

        const queries = batch.streamMessages.map((streamMessage) => {
            return {
                query: this.insertStatement,
                params: [
                    streamMessage.getStreamId(),
                    streamMessage.getStreamPartition(),
                    streamMessage.getTimestamp(),
                    streamMessage.getSequenceNumber(),
                    streamMessage.getPublisherId(),
                    streamMessage.getMsgChainId(),
                    Buffer.from(streamMessage.serialize()),
                ]
            }
        })

        try {
            await this.cassandraClient.batch(queries, {
                prepare: true
            })

            batch.clear()
            delete this.pendingBatches[batch.id]
        } catch (e) {
            const key = `${batch.streamMessages[0].getStreamId()}::${batch.streamMessages[0].getStreamPartition()}`
            if (this.logErrors) {
                console.error(`Failed to insert (${key}): ${e.stack ? e.stack : e}`)
            }

            batch.scheduleRetry()
        }
    }
}

module.exports = BatchManager
