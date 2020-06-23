const EventEmitter = require('events')

const debug = require('debug')('streamr:storage:batch-manager')

const Batch = require('./Batch')

const INSERT_STATEMENT = 'INSERT INTO stream_data_new '
    + '(stream_id, partition, bucket_id, ts, sequence_no, publisher_id, msg_chain_id, payload) '
    + 'VALUES (?, ?, ?, ?, ?, ?, ?, ?)'

const INSERT_STATEMENT_WITH_TTL = 'INSERT INTO stream_data_new '
    + '(stream_id, partition, bucket_id, ts, sequence_no, publisher_id, msg_chain_id, payload) '
    + 'VALUES (?, ?, ?, ?, ?, ?, ?, ?) USING TTL 259200' // 3 days

class BatchManager extends EventEmitter {
    constructor(cassandraClient, opts) {
        super()

        const defaultOptions = {
            useTtl: false,
            logErrors: false,
            batchMaxSize: 8000 * 300,
            batchMaxRecords: 8000,
            batchCloseTimeout: 1000,
            batchMaxRetries: 1000 // in total max ~16 minutes timeout
        }

        this.opts = {
            ...defaultOptions,
            ...opts
        }

        // bucketId => batch
        this.batches = {}
        // batchId => batch
        this.pendingBatches = {}

        this.cassandraClient = cassandraClient
        this.insertStatement = this.opts.useTtl ? INSERT_STATEMENT_WITH_TTL : INSERT_STATEMENT
    }

    store(bucketId, streamMessage) {
        const batch = this.batches[bucketId]

        if (batch && batch.isFull()) {
            batch.setClose(false)
            this._moveFullBatch(bucketId, batch)
        }

        if (this.batches[bucketId] === undefined) {
            debug('creating new batch')
            const newBatch = new Batch(bucketId, this.opts.batchMaxSize, this.opts.batchMaxRecords, this.opts.batchCloseTimeout, this.opts.batchMaxRetries)
            newBatch.on('state', (batchBucketId, id, state, size, numberOfRecords) => this._batchChangedState(batchBucketId, id, state, size, numberOfRecords))
            this.batches[bucketId] = newBatch
        }

        this.batches[bucketId].push(streamMessage)
    }

    _moveFullBatch(bucketId, batch) {
        debug('moving batch to pendingBatches')
        this.pendingBatches[batch.getId()] = batch
        this.pendingBatches[batch.getId()].scheduleInsert()
        delete this.batches[bucketId]
    }

    _batchChangedState(bucketId, batchId, state, size, numberOfRecords) {
        debug(`_batchChangedState, bucketId: ${bucketId}, id: ${batchId}, state: ${state}, size: ${size}, records: ${numberOfRecords}`)
        if (state === Batch.states.PENDING) {
            this._insert(batchId)
        } else if (state === Batch.states.CLOSED) {
            const batch = this.batches[bucketId]
            if (batch) {
                this._moveFullBatch(bucketId, batch)
            }
        }
    }

    stop() {
        Object.values(this.batches).forEach((batch) => batch.clear())
        Object.values(this.pendingBatches).forEach((batch) => batch.clear())
    }

    async _insert(batchId) {
        const batch = this.pendingBatches[batchId]

        try {
            const queries = batch.streamMessages.map((streamMessage) => {
                return {
                    query: this.insertStatement,
                    params: [
                        streamMessage.getStreamId(),
                        streamMessage.getStreamPartition(),
                        batch.getBucketId(),
                        streamMessage.getTimestamp(),
                        streamMessage.getSequenceNumber(),
                        streamMessage.getPublisherId(),
                        streamMessage.getMsgChainId(),
                        Buffer.from(streamMessage.serialize()),
                    ]
                }
            })

            await this.cassandraClient.batch(queries, {
                prepare: true
            })

            debug(`inserted batch id:${batch.getId()}`)
            batch.clear()
            delete this.pendingBatches[batch.getId()]
        } catch (e) {
            debug(`failed to insert batch, error ${e}`)
            if (this.opts.logErrors) {
                console.error(`Failed to insert batchId: (${batchId})`)
                console.error(e)
            }

            if (batch.reachedMaxRetries()) {
                console.error(`Batch ${batchId} reached max retries, dropping batch`)
                batch.clear()
                delete this.pendingBatches[batch.getId()]
                return
            }
            batch.scheduleInsert()
        }
    }
}

module.exports = BatchManager
