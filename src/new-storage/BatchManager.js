const EventEmitter = require('events')

const Batch = require('./Batch')

const INSERT_STATEMENT = 'INSERT INTO stream_data_new '
    + '(stream_id, partition, bucket_id, ts, sequence_no, publisher_id, msg_chain_id, payload) '
    + 'VALUES (?, ?, ?, ?, ?, ?, ?, ?)'

const INSERT_STATEMENT_WITH_TTL = 'INSERT INTO stream_data_new '
    + '(stream_id, partition, bucket_id, ts, sequence_no, publisher_id, msg_chain_id, payload) '
    + 'VALUES (?, ?, ?, ?, ?, ?, ?, ?) USING TTL 259200' // 3 days

class BatchManager extends EventEmitter {
    constructor(cassandraClient, useTtl = true, logErrors = true) {
        super()

        this.batches = {}
        this.pendingBatches = {}

        this.cassandraClient = cassandraClient
        this.insertStatement = useTtl ? INSERT_STATEMENT_WITH_TTL : INSERT_STATEMENT
        this.logErrors = logErrors
    }

    store(bucketId, streamMessage) {
        const batch = this.batches[bucketId]

        if (batch && batch.isFull()) {
            batch.close()

            this.pendingBatches[batch.id] = batch
            this.pendingBatches[batch.id].setPending()

            this.batches[bucketId] = undefined
        }

        if (this.batches[bucketId] === undefined) {
            const newBatch = new Batch(bucketId, 10000, 10, 30000)
            newBatch.on('state', (id, state, size, numberOfRecords) => this._batchChangedState(id, state, size, numberOfRecords))
            this.batches[bucketId] = newBatch
        }

        this.batches[bucketId].push(streamMessage)
    }

    _batchChangedState(id, state, size, numberOfRecords) {
        console.log(`batchId: ${id}, batchState: ${state}, batchSize: ${size}, batchNumberOfRecords: ${numberOfRecords}`)
        if (state === Batch.states.PENDING) {
            this._insert(id)
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
                    batch.bucketId,
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
