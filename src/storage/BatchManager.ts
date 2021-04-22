import { Client } from 'cassandra-driver'
import { EventEmitter } from 'events'
import { Protocol } from 'streamr-network'
import getLogger from '../helpers/logger'
import { Todo } from '../types'
import { Batch, BatchId, DoneCallback } from './Batch'
import { BucketId } from './Bucket'

const logger = getLogger('streamr:storage:BatchManager')

const INSERT_STATEMENT = 'INSERT INTO stream_data '
    + '(stream_id, partition, bucket_id, ts, sequence_no, publisher_id, msg_chain_id, payload) '
    + 'VALUES (?, ?, ?, ?, ?, ?, ?, ?)'

const INSERT_STATEMENT_WITH_TTL = 'INSERT INTO stream_data '
    + '(stream_id, partition, bucket_id, ts, sequence_no, publisher_id, msg_chain_id, payload) '
    + 'VALUES (?, ?, ?, ?, ?, ?, ?, ?) USING TTL 259200' // 3 days

export class BatchManager extends EventEmitter {

    opts: Todo
    batches: Record<BucketId,Batch>
    pendingBatches: Record<BatchId,Batch>
    cassandraClient: Client
    insertStatement: string

    constructor(cassandraClient: Client, opts = {}) {
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

    store(bucketId: BucketId, streamMessage: Protocol.StreamMessage, doneCb?: DoneCallback) {
        const batch = this.batches[bucketId]

        if (batch && batch.isFull()) {
            batch.lock()
        }

        if (this.batches[bucketId] === undefined) {
            logger.debug('creating new batch')

            const newBatch = new Batch(bucketId, this.opts.batchMaxSize, this.opts.batchMaxRecords, this.opts.batchCloseTimeout, this.opts.batchMaxRetries)

            newBatch.on('locked', () => this._moveFullBatch(bucketId, newBatch))
            newBatch.on('pending', () => this._insert(newBatch.getId()))

            this.batches[bucketId] = newBatch
        }

        this.batches[bucketId].push(streamMessage, doneCb)
    }

    _moveFullBatch(bucketId: BucketId, batch: Batch) {
        logger.debug('moving batch to pendingBatches')

        this.pendingBatches[batch.getId()] = batch
        this.pendingBatches[batch.getId()].scheduleInsert()

        delete this.batches[bucketId]
    }

    stop() {
        Object.values(this.batches).forEach((batch) => batch.clear())
        Object.values(this.pendingBatches).forEach((batch) => batch.clear())
    }

    async _insert(batchId: BatchId) {
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

            logger.debug(`inserted batch id:${batch.getId()}`)
            batch.done()
            batch.clear()
            delete this.pendingBatches[batch.getId()]
        } catch (e) {
            logger.debug(`failed to insert batch, error ${e}`)
            if (this.opts.logErrors) {
                logger.error(`Failed to insert batchId: (${batchId})`)
                logger.error(e)
            }

            if (batch.reachedMaxRetries()) {
                if (this.opts.logErrors) {
                    logger.error(`Batch ${batchId} reached max retries, dropping batch`)
                }
                batch.clear()
                delete this.pendingBatches[batch.getId()]
                return
            }
            batch.scheduleInsert()
        }
    }

    metrics() {
        const now = Date.now()
        const totalBatches = Object.values(this.batches).length + Object.values(this.pendingBatches).length
        const meanBatchAge = totalBatches === 0 ? 0
            : [...Object.values(this.batches), ...Object.values(this.pendingBatches)].reduce((acc, batch) => acc + (now - batch.createdAt), 0) / totalBatches
        const meanBatchRetries = totalBatches === 0 ? 0
            : Object.values(this.pendingBatches).reduce((acc, batch) => acc + batch.retries, 0) / totalBatches

        let batchesWithFiveOrMoreRetries = 0
        let batchesWithTenOrMoreRetries = 0
        let batchesWithHundredOrMoreRetries = 0

        Object.values(this.pendingBatches).forEach((batch) => {
            if (batch.retries >= 5) {
                batchesWithFiveOrMoreRetries += 1
                if (batch.retries >= 10) {
                    batchesWithTenOrMoreRetries += 1
                    if (batch.retries >= 100) {
                        batchesWithHundredOrMoreRetries += 1
                    }
                }
            }
        })

        return {
            totalBatches,
            meanBatchAge,
            meanBatchRetries,
            batchesWithFiveOrMoreRetries,
            batchesWithTenOrMoreRetries,
            batchesWithHundredOrMoreRetries
        }
    }
}
