const { Readable, Transform } = require('stream')
const EventEmitter = require('events')

const debug = require('debug')('streamr:storage')
const NodeCache = require('node-cache')
const cassandra = require('cassandra-driver')
const pump = require('pump')
const { StreamMessageFactory } = require('streamr-client-protocol').MessageLayer

const BatchManager = require('./BatchManager')
const BucketManager = require('./BucketManager')

const MAX_RESEND_LAST = 10000

class Storage extends EventEmitter {
    constructor(cassandraClient, useTtl) {
        super()

        this.cassandraClient = cassandraClient
        this.batchManager = new BatchManager(cassandraClient, useTtl, true)
        this.bucketManager = new BucketManager(cassandraClient, true)

        this.pendingMessages = new NodeCache({
            stdTTL: 3,
            checkperiod: 3
        })

        this.pendingMessages.on('expired', (messageId, streamMessage) => {
            setImmediate(() => {
                debug(`pendingMessage expired: ${messageId}, store again`)
                this.store(streamMessage)
            })
        })
    }

    store(streamMessage) {
        const bucketId = this.bucketManager.getBucketId(streamMessage.getStreamId(), streamMessage.getStreamPartition(), streamMessage.getTimestamp())

        if (bucketId) {
            debug(`found bucketId: ${bucketId}`)
            this.batchManager.store(bucketId, streamMessage)
            this.bucketManager.incrementBucket(bucketId, Buffer.from(streamMessage.serialize()).length, 1)
        } else {
            const messageId = streamMessage.messageId.serialize()
            debug(`bucket not found, put ${messageId} to pendingMessages`)
            this.pendingMessages.set(messageId, streamMessage)
        }
    }

    requestLast(streamId, partition, limit) {
        debug(`requestLast, streamId: "${streamId}", partition: "${partition}", limit: "${limit}"`)

        const GET_LAST_N_MESSAGES = 'SELECT * FROM stream_data_new WHERE '
                                  + 'stream_id = ? AND partition = ? AND bucket_id IN ? '
                                  + 'ORDER BY ts DESC, sequence_no DESC '
                                  + 'LIMIT ?'
        const GET_BUCKETS = 'SELECT id, records FROM bucket WHERE stream_id = ? AND partition = ?'
        const resultStream = this._createResultStream()

        let total = 0
        const bucketsForQuery = []
        const options = {
            prepare: true, fetchSize: 10
        }

        const makeLastQuery = () => {
            const params = [streamId, partition, bucketsForQuery, limit]
            debug(`requestLast: ${GET_LAST_N_MESSAGES}, params: ${params}`)

            this.cassandraClient.execute(GET_LAST_N_MESSAGES, params, {
                prepare: true,
                fetchSize: 0 // disable paging
            }).then((resultSet) => {
                resultSet.rows.reverse().forEach((r) => {
                    resultStream.write(r)
                })
                resultStream.push(null)
            }).catch((e) => {
                console.error(e)
                resultStream.push(null)
            })
        }

        // eachRow is used to get needed amount of buckets dynamically
        this.cassandraClient.eachRow(GET_BUCKETS, [streamId, partition], options, (n, row) => {
            if (total <= limit) {
                total += row.records
                bucketsForQuery.push(row.id)
            }
        }, (err, result) => {
            if (err) {
                console.error(err)
            }

            // if fetched buckets, but found not enough records => continue
            if (result.nextPage && total < limit && total < MAX_RESEND_LAST) {
                result.nextPage()
            } else {
                makeLastQuery()
            }
        })

        return resultStream
    }

    requestFrom(streamId, streamPartition, fromTimestamp, fromSequenceNo, publisherId, msgChainId) {
        if (fromSequenceNo != null && publisherId != null && msgChainId != null) {
            return this._fetchFromMessageRefForPublisher(streamId, streamPartition, fromTimestamp,
                fromSequenceNo, publisherId, msgChainId)
        }
        if ((fromSequenceNo == null || fromSequenceNo === 0) && publisherId == null && msgChainId == null) {
            return this._fetchFromTimestamp(streamId, streamPartition, fromTimestamp)
        }

        throw new Error('Invalid combination of requestFrom arguments')
    }

    _fetchFromTimestamp(streamId, partition, fromTimestamp) {
        const resultStream = this._createResultStream()

        const query = 'SELECT * FROM stream_data_new WHERE '
                    + 'stream_id = ? AND partition = ? AND bucket_id IN ? AND ts >= ?'

        this.bucketManager.getBucketsFromTimestamp(streamId, partition, fromTimestamp).then((buckets) => {
            const bucketsForQuery = []

            for (let i = 0; i < buckets.length; i++) {
                const bucket = buckets[i]
                bucketsForQuery.push(bucket.id)
            }

            const queryParams = [streamId, partition, bucketsForQuery, fromTimestamp]
            const cassandraStream = this._queryWithStreamingResults(query, queryParams)

            return pump(
                cassandraStream,
                resultStream,
                (err) => {
                    if (err) {
                        console.error('pump finished with error', err)
                        resultStream.push(null)
                    }
                }
            )
        }).catch((e) => {
            console.error(e)
            resultStream.push(null)
        })

        return resultStream
    }
    //
    // _fetchFromMessageRefForPublisher(streamId, streamPartition, fromTimestamp, fromSequenceNo, publisherId, msgChainId) {
    //     // Cassandra doesn't allow ORs in WHERE clause so we need to do 2 queries.
    //     // Once a range (id/partition/ts/sequence_no) has been selected in Cassandra, filtering it by publisher_id requires to ALLOW FILTERING.
    //     const query1 = 'SELECT * FROM stream_data WHERE id = ? AND partition = ? AND ts = ? AND sequence_no >= ? AND publisher_id = ? '
    //         + 'AND msg_chain_id = ? ORDER BY ts ASC, sequence_no ASC ALLOW FILTERING'
    //     const query2 = 'SELECT * FROM stream_data WHERE id = ? AND partition = ? AND ts > ? AND publisher_id = ? '
    //         + 'AND msg_chain_id = ? ORDER BY ts ASC, sequence_no ASC ALLOW FILTERING'
    //     const queryParams1 = [streamId, streamPartition, fromTimestamp, fromSequenceNo, publisherId, msgChainId]
    //     const queryParams2 = [streamId, streamPartition, fromTimestamp, publisherId, msgChainId]
    //     const stream1 = this._queryWithStreamingResults(query1, queryParams1)
    //     const stream2 = this._queryWithStreamingResults(query2, queryParams2)
    //     return merge2(stream1, stream2)
    // }

    requestRange(
        streamId,
        streamPartition,
        fromTimestamp,
        fromSequenceNo,
        toTimestamp,
        toSequenceNo,
        publisherId,
        msgChainId
    ) {
        if (!Number.isInteger(fromTimestamp)) {
            throw new Error('fromTimestamp is not an integer')
        }
        if (fromSequenceNo != null && !Number.isInteger(fromSequenceNo)) {
            throw new Error('fromSequenceNo is not an integer')
        }
        if (!Number.isInteger(toTimestamp)) {
            throw new Error('toTimestamp is not an integer')
        }
        if (toSequenceNo != null && !Number.isInteger(toSequenceNo)) {
            throw new Error('toSequenceNo is not an integer')
        }

        if (fromSequenceNo != null && toSequenceNo != null && publisherId != null && msgChainId != null) {
            // if (toTimestamp > (Date.now() - RANGE_THRESHOLD)) {
            //     const periodicQuery = new PeriodicQuery(() => this._fetchBetweenMessageRefsForPublisher(streamId, streamPartition, fromTimestamp,
            //         fromSequenceNo, toTimestamp, toSequenceNo, publisherId, msgChainId), RETRY_INTERVAL, RETRY_TIMEOUT)
            //     return periodicQuery.getStreamingResults()
            // }
            // return this._fetchBetweenMessageRefsForPublisher(streamId, streamPartition, fromTimestamp,
            //     fromSequenceNo, toTimestamp, toSequenceNo, publisherId, msgChainId)
        }
        if ((fromSequenceNo == null || fromSequenceNo === 0) && (toSequenceNo == null || toSequenceNo === 0)
            && publisherId == null && msgChainId == null) {
            return this._fetchBetweenTimestamps(streamId, streamPartition, fromTimestamp, toTimestamp)
        }

        throw new Error('Invalid combination of requestFrom arguments')
    }

    // _fetchBetweenTimestamps(streamId, streamPartition, from, to) {
    //     if (!Number.isInteger(from)) {
    //         throw new Error('from is not an integer')
    //     }
    //
    //     if (!Number.isInteger(to)) {
    //         throw new Error('to is not an integer')
    //     }
    //
    //     const query = 'SELECT * FROM stream_data WHERE id = ? AND partition = ? AND ts >= ? AND ts <= ? ORDER BY ts ASC, sequence_no ASC'
    //     const queryParams = [streamId, streamPartition, from, to]
    //     return this._queryWithStreamingResults(query, queryParams)
    // }
    //
    // _fetchBetweenMessageRefsForPublisher(
    //     streamId,
    //     streamPartition,
    //     fromTimestamp,
    //     fromSequenceNo,
    //     toTimestamp,
    //     toSequenceNo,
    //     publisherId,
    //     msgChainId
    // ) {
    //     // Cassandra doesn't allow ORs in WHERE clause so we need to do 3 queries.
    //     // Once a range (id/partition/ts/sequence_no) has been selected in Cassandra, filtering it by publisher_id requires to ALLOW FILTERING.
    //     const query1 = 'SELECT * FROM stream_data WHERE id = ? AND partition = ? AND ts = ? AND sequence_no >= ? AND publisher_id = ? '
    //         + 'AND msg_chain_id = ? ORDER BY ts ASC, sequence_no ASC ALLOW FILTERING'
    //     const query2 = 'SELECT * FROM stream_data WHERE id = ? AND partition = ? AND ts > ? AND ts < ? AND publisher_id = ? '
    //         + 'AND msg_chain_id = ? ORDER BY ts ASC, sequence_no ASC ALLOW FILTERING'
    //     const query3 = 'SELECT * FROM stream_data WHERE id = ? AND partition = ? AND ts = ? AND sequence_no <= ? AND publisher_id = ? '
    //         + 'AND msg_chain_id = ? ORDER BY ts ASC, sequence_no ASC ALLOW FILTERING'
    //     const queryParams1 = [streamId, streamPartition, fromTimestamp, fromSequenceNo, publisherId, msgChainId]
    //     const queryParams2 = [streamId, streamPartition, fromTimestamp, toTimestamp, publisherId, msgChainId]
    //     const queryParams3 = [streamId, streamPartition, toTimestamp, toSequenceNo, publisherId, msgChainId]
    //     const stream1 = this._queryWithStreamingResults(query1, queryParams1)
    //     const stream2 = this._queryWithStreamingResults(query2, queryParams2)
    //     const stream3 = this._queryWithStreamingResults(query3, queryParams3)
    //     return merge2(stream1, stream2, stream3)
    // }

    // eslint-disable-next-line class-methods-use-this
    metrics() {
        return {
            storeStrategy: undefined // this.storeStrategy.metrics()
        }
    }

    close() {
        // TODO close properly buckets and batches
        this.storeStrategy.close()
        return this.cassandraClient.shutdown()
    }

    _queryWithStreamingResults(query, queryParams) {
        const cassandraStream = this.cassandraClient.stream(query, queryParams, {
            prepare: true,
            autoPage: true
        })

        // To avoid blocking main thread for too long, on every 1000th message
        // pause & resume the cassandraStream to give other events in the event
        // queue a chance to be handled.
        let resultCount = 0
        cassandraStream.on('data', () => {
            resultCount += 1
            if (resultCount % 1000 === 0) {
                cassandraStream.pause()
                setImmediate(() => cassandraStream.resume())
            }
        }).on('error', (err) => {
            console.error(err)
        })

        return cassandraStream
    }

    _parseRow(row) {
        const streamMessage = StreamMessageFactory.deserialize(row.payload.toString())
        this.emit('read', streamMessage)
        return streamMessage
    }

    _createResultStream() {
        return new Transform({
            objectMode: true,
            transform: (row, _, done) => {
                done(null, this._parseRow(row))
            }
        })
    }
}

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms))
}

const startCassandraStorage = async ({
    contactPoints,
    localDataCenter,
    keyspace,
    username,
    password,
    useTtl = true
}) => {
    const authProvider = new cassandra.auth.PlainTextAuthProvider(username || '', password || '')
    const requestLogger = new cassandra.tracker.RequestLogger({
        slowThreshold: 10 * 1000, // 10 secs
    })
    requestLogger.emitter.on('slow', (message) => console.warn(message))
    const cassandraClient = new cassandra.Client({
        contactPoints,
        localDataCenter,
        keyspace,
        authProvider,
        requestLogger,
        pooling: {
            maxRequestsPerConnection: 32768
        }
    })
    const nbTrials = 20
    let retryCount = nbTrials
    let lastError = ''
    while (retryCount > 0) {
        /* eslint-disable no-await-in-loop */
        try {
            await cassandraClient.connect().catch((err) => { throw err })
            return new Storage(cassandraClient, useTtl)
        } catch (err) {
            console.log('Cassandra not responding yet...')
            retryCount -= 1
            await sleep(5000)
            lastError = err
        }
        /* eslint-enable no-await-in-loop */
    }
    throw new Error(`Failed to connect to Cassandra after ${nbTrials} trials: ${lastError.toString()}`)
}

module.exports = {
    Storage,
    startCassandraStorage,
}
