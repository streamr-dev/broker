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
            batchMaxSize: 10000,
            batchMaxRecords: 10,
            batchCloseTimeout: 1000,
            batchMaxRetries: 64
        }

        this.opts = {
            ...defaultOptions, ...opts
        }

        this.batches = {}
        this.pendingBatches = {}

        this.cassandraClient = cassandraClient
        this.insertStatement = this.opts.useTtl ? INSERT_STATEMENT_WITH_TTL : INSERT_STATEMENT
    }

    store(bucketId, streamMessage) {
        const batch = this.batches[bucketId]

        if (batch && batch.isFull()) {
            batch.setClose()
            this._moveFullBatch(batch, bucketId)
        }

        if (this.batches[bucketId] === undefined) {
            debug('creating new batch')
            const newBatch = new Batch(bucketId, this.opts.batchMaxSize, this.opts.batchMaxRecords, this.opts.batchCloseTimeout, this.opts.batchMaxRetries)
            newBatch.on('state', (batchBucketId, id, state, size, numberOfRecords) => this._batchChangedState(bucketId, id, state, size, numberOfRecords))
            this.batches[bucketId] = newBatch
        }

        this.batches[bucketId].push(streamMessage)
    }

    _moveFullBatch(batch, bucketId) {
        debug('batch is full, closing')
        this.pendingBatches[batch.getId()] = batch
        this.pendingBatches[batch.getId()].scheduleInsert()
        this.batches[bucketId] = undefined
    }

    _batchChangedState(bucketId, id, state, size, numberOfRecords) {
        debug(`_batchChangedState, id: ${id}, state: ${state}, size: ${size}, records: ${numberOfRecords}`)
        if (state === Batch.states.PENDING) {
            this._insert(id)
        } else if (state === Batch.states.CLOSED) {
            const batch = this.batches[bucketId]
            if (batch) {
                this._moveFullBatch(batch, bucketId)
            }
        }
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

            batch.clear()
            delete this.pendingBatches[batch.getId()]
            debug(`inserted ${batch.getId()}`)
        } catch (e) {
            const key = `${batch.streamMessages[0].getStreamId()}::${batch.streamMessages[0].getStreamPartition()}`
            if (this.opts.logErrors) {
                console.error(`Failed to insert (${key}): ${e.stack ? e.stack : e}`)
            }
            debug(`failed to insert ${batch.getId()}, error ${e}`)
            batch.scheduleInsert()
        }
    }
}

module.exports = BatchManager
