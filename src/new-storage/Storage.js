const { Transform } = require('stream')
const EventEmitter = require('events')

const debug = require('debug')('streamr:storage')
const NodeCache = require('node-cache')
const cassandra = require('cassandra-driver')
const merge2 = require('merge2')
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
            stdTTL: 1,
            checkperiod: 1
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

            this.bucketManager.incrementBucket(bucketId, Buffer.from(streamMessage.serialize()).length, 1)
            setImmediate(() => this.batchManager.store(bucketId, streamMessage))
        } else {
            const messageId = streamMessage.messageId.serialize()
            debug(`bucket not found, put ${messageId} to pendingMessages`)
            this.pendingMessages.set(messageId, streamMessage)
        }
    }

    requestLast(streamId, partition, limit) {
        debug(`requestLast, streamId: "${streamId}", partition: "${partition}", limit: "${limit}"`)

        const GET_LAST_N_MESSAGES = 'SELECT payload FROM stream_data_new WHERE '
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
            debug(`requestLast query: ${GET_LAST_N_MESSAGES}, params: ${params}`)

            if (!bucketsForQuery.length) {
                resultStream.push(null)
            } else {
                this.cassandraClient.execute(GET_LAST_N_MESSAGES, params, {
                    prepare: true,
                    fetchSize: 0 // disable paging
                }).then((resultSet) => {
                    resultSet.rows.reverse().forEach((r) => {
                        resultStream.write(r)
                    })
                    resultStream.push(null)
                }).catch((e) => {
                    console.warn(e)
                    resultStream.push(null)
                })
            }
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

    requestFrom(streamId, partition, fromTimestamp, fromSequenceNo, publisherId, msgChainId) {
        debug(`requestFrom, streamId: "${streamId}", partition: "${partition}", fromTimestamp: "${fromTimestamp}", fromSequenceNo: `
            + `"${fromSequenceNo}", publisherId: "${publisherId}", msgChainId: "${msgChainId}"`)

        if (fromSequenceNo != null && publisherId != null && msgChainId != null) {
            return this._fetchFromMessageRefForPublisher(streamId, partition, fromTimestamp,
                fromSequenceNo, publisherId, msgChainId)
        }
        if ((fromSequenceNo == null || fromSequenceNo === 0) && publisherId == null && msgChainId == null) {
            return this._fetchFromTimestamp(streamId, partition, fromTimestamp)
        }

        throw new Error('Invalid combination of requestFrom arguments')
    }

    requestRange(streamId, partition, fromTimestamp, fromSequenceNo, toTimestamp, toSequenceNo, publisherId, msgChainId) {
        debug(`requestRange, streamId: "${streamId}", partition: "${partition}", fromTimestamp: "${fromTimestamp}", fromSequenceNo: "${fromSequenceNo}"`
            + `, toTimestamp: "${toTimestamp}", toSequenceNo: "${toSequenceNo}", publisherId: "${publisherId}", msgChainId: "${msgChainId}"`)

        if (fromSequenceNo != null && toSequenceNo != null && publisherId != null && msgChainId != null) {
            return this._fetchBetweenMessageRefsForPublisher(streamId, partition, fromTimestamp,
                fromSequenceNo, toTimestamp, toSequenceNo, publisherId, msgChainId)
        }
        if ((fromSequenceNo == null || fromSequenceNo === 0)
            && (toSequenceNo == null || toSequenceNo === 0)
            && publisherId == null && msgChainId == null) {
            return this._fetchBetweenTimestamps(streamId, partition, fromTimestamp, toTimestamp)
        }

        throw new Error('Invalid combination of requestFrom arguments')
    }

    _fetchFromTimestamp(streamId, partition, fromTimestamp) {
        const resultStream = this._createResultStream()

        const query = 'SELECT payload FROM stream_data_new WHERE '
                    + 'stream_id = ? AND partition = ? AND bucket_id IN ? AND ts >= ?'

        this.bucketManager.getLastBuckets(streamId, partition, 1, fromTimestamp).then((buckets) => {
            return buckets.length ? buckets[0].dateCreate : fromTimestamp
        }).then((startBucketTimestamp) => {
            return startBucketTimestamp ? this.bucketManager.getBucketsByTimestamp(streamId, partition, startBucketTimestamp) : []
        }).then((buckets) => {
            if (!buckets.length) {
                return resultStream.push(null)
            }
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
        })
            .catch((e) => {
                console.warn(e)
                resultStream.push(null)
            })

        return resultStream
    }

    _fetchBetweenTimestamps(streamId, partition, fromTimestamp, toTimestamp) {
        const resultStream = this._createResultStream()

        const query = 'SELECT payload FROM stream_data_new WHERE '
                    + 'stream_id = ? AND partition = ? AND bucket_id IN ? AND ts >= ? AND ts <= ?'

        Promise.all([
            this.bucketManager.getLastBuckets(streamId, partition, 1, fromTimestamp),
            this.bucketManager.getLastBuckets(streamId, partition, 1, toTimestamp),
        ]).then((results) => {
            const date1 = results[0][0].dateCreate
            const date2 = results[1][0].dateCreate
            return [Math.min(date1, date2), Math.max(date1, date2)]
        }).then(([startBucketDate, endBucketDate]) => {
            return this.bucketManager.getBucketsByTimestamp(streamId, partition, startBucketDate, endBucketDate)
        }).then((buckets) => {
            const bucketsForQuery = []

            for (let i = 0; i < buckets.length; i++) {
                const bucket = buckets[i]
                bucketsForQuery.push(bucket.id)
            }

            const queryParams = [streamId, partition, bucketsForQuery, fromTimestamp, toTimestamp]
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
        })
            .catch((e) => {
                console.warn(e)
                resultStream.push(null)
            })

        return resultStream
    }

    _fetchFromMessageRefForPublisher(streamId, partition, fromTimestamp, fromSequenceNo, publisherId, msgChainId) {
        const resultStream = this._createResultStream()

        const query1 = 'SELECT payload FROM stream_data_new WHERE stream_id = ? AND partition = ? AND bucket_id IN ? AND ts = ? AND sequence_no >= ? AND publisher_id = ? '
            + 'AND msg_chain_id = ? ALLOW FILTERING'
        const query2 = 'SELECT payload FROM stream_data_new WHERE stream_id = ? AND partition = ? AND bucket_id IN ? AND ts > ? AND publisher_id = ? '
            + 'AND msg_chain_id = ? ALLOW FILTERING'

        this.bucketManager.getLastBuckets(streamId, partition, 1, fromTimestamp).then((buckets) => {
            return buckets.length ? buckets[0].dateCreate : undefined
        }).then((startBucketTimestamp) => {
            return startBucketTimestamp ? this.bucketManager.getBucketsByTimestamp(streamId, partition, startBucketTimestamp) : []
        }).then((buckets) => {
            if (!buckets.length) {
                return resultStream.push(null)
            }
            const bucketsForQuery = []

            for (let i = 0; i < buckets.length; i++) {
                const bucket = buckets[i]
                bucketsForQuery.push(bucket.id)
            }

            const queryParams1 = [streamId, partition, bucketsForQuery, fromTimestamp, fromSequenceNo, publisherId, msgChainId]
            const queryParams2 = [streamId, partition, bucketsForQuery, fromTimestamp, publisherId, msgChainId]
            const stream1 = this._queryWithStreamingResults(query1, queryParams1)
            const stream2 = this._queryWithStreamingResults(query2, queryParams2)

            return pump(
                merge2(stream1, stream2),
                resultStream,
                (err) => {
                    if (err) {
                        console.error('pump finished with error', err)
                        resultStream.push(null)
                    }
                }
            )
        })
            .catch((e) => {
                console.warn(e)
                resultStream.push(null)
            })

        return resultStream
    }

    _fetchBetweenMessageRefsForPublisher(streamId, partition, fromTimestamp, fromSequenceNo, toTimestamp, toSequenceNo, publisherId, msgChainId) {
        const resultStream = this._createResultStream()

        const query1 = 'SELECT payload FROM stream_data_new WHERE stream_id = ? AND partition = ? AND bucket_id IN ? AND ts = ? AND sequence_no >= ? AND publisher_id = ? '
            + 'AND msg_chain_id = ? ALLOW FILTERING'
        const query2 = 'SELECT payload FROM stream_data_new WHERE stream_id = ? AND partition = ? AND bucket_id IN ? AND ts > ? AND ts < ? AND publisher_id = ? '
            + 'AND msg_chain_id = ? ALLOW FILTERING'
        const query3 = 'SELECT payload FROM stream_data_new WHERE stream_id = ? AND partition = ? AND bucket_id IN ? AND ts = ? AND sequence_no <= ? AND publisher_id = ? '
            + 'AND msg_chain_id = ? ALLOW FILTERING'

        // TODO replace with allSettled
        Promise.all([
            this.bucketManager.getLastBuckets(streamId, partition, 1, fromTimestamp),
            this.bucketManager.getLastBuckets(streamId, partition, 1, toTimestamp),
        ]).then((results) => {
            const date1 = results[0][0].dateCreate
            const date2 = results[1][0].dateCreate
            return [Math.min(date1, date2), Math.max(date1, date2)]
        }).then(([startBucketDate, endBucketDate]) => {
            return this.bucketManager.getBucketsByTimestamp(streamId, partition, startBucketDate, endBucketDate)
        }).then((buckets) => {
            const bucketsForQuery = []

            for (let i = 0; i < buckets.length; i++) {
                const bucket = buckets[i]
                bucketsForQuery.push(bucket.id)
            }

            const queryParams1 = [streamId, partition, bucketsForQuery, fromTimestamp, fromSequenceNo, publisherId, msgChainId]
            const queryParams2 = [streamId, partition, bucketsForQuery, fromTimestamp, toTimestamp, publisherId, msgChainId]
            const queryParams3 = [streamId, partition, bucketsForQuery, toTimestamp, toSequenceNo, publisherId, msgChainId]
            const stream1 = this._queryWithStreamingResults(query1, queryParams1)
            const stream2 = this._queryWithStreamingResults(query2, queryParams2)
            const stream3 = this._queryWithStreamingResults(query3, queryParams3)

            return pump(
                merge2(stream1, stream2, stream3),
                resultStream,
                (err) => {
                    if (err) {
                        console.error('pump finished with error', err)
                        resultStream.push(null)
                    }
                }
            )
        })
            .catch((e) => {
                console.warn(e)
                resultStream.push(null)
            })

        return resultStream
    }

    // eslint-disable-next-line class-methods-use-this
    metrics() {
        return {
            storeStrategy: undefined // this.storeStrategy.metrics()
        }
    }

    close() {
        this.batchManager.stop()
        this.bucketManager.stop()
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
