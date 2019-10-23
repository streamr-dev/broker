const { Readable, Transform } = require('stream')

const merge2 = require('merge2')
const { Pool } = require('pg')
const { StreamMessageFactory } = require('streamr-client-protocol').MessageLayer

const MicroBatchingStrategy = require('./MicroBatchingStrategy')

const INSERT_STATEMENT = 'INSERT INTO stream_data '
    + '(id, partition, ts, timestamp, sequence_no, publisher_id, msg_chain_id, payload) '
    + 'VALUES ($1, $2, $3, NOW(), $4, $5, $6, $7)'

const INSERT_STATEMENT_WITH_TTL = 'INSERT INTO stream_data '
    + '(id, partition, ts, sequence_no, publisher_id, msg_chain_id, payload) '
    + 'VALUES (?, ?, ?, ?, ?, ?, ?) USING TTL 259200' // 3 days

const batchingStore = (cassandraClient, insertStatement) => new MicroBatchingStrategy({
    insertFn: (streamMessages) => {
        const queries = streamMessages.map((streamMessage) => {
            return {
                query: insertStatement,
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
        return cassandraClient.batch(queries, {
            prepare: true
        })
    }
})

const individualStore = (cassandraClient, insertStatement) => ({
    store: (streamMessage) => {
        return cassandraClient.query('BEGIN', (err) => {
            const values = [
                streamMessage.getStreamId(),
                streamMessage.getStreamPartition(),
                streamMessage.getTimestamp(),
                streamMessage.getSequenceNumber(),
                streamMessage.getPublisherId(),
                streamMessage.getMsgChainId(),
                streamMessage.serialize(),
            ]
            // eslint-disable-next-line no-shadow
            cassandraClient.query(insertStatement, values, (err, res) => {
                // eslint-disable-next-line no-shadow
                cassandraClient.query('COMMIT', (err) => {
                    if (err) {
                        console.error('Error committing transaction', err.stack)
                    }
                })
            })
        })
    },
    close: () => {}
})

const parseRow = (row) => {
    const streamMessage = StreamMessageFactory.deserialize(row.payload.toString())
    return {
        streamId: streamMessage.getStreamId(),
        streamPartition: streamMessage.getStreamPartition(),
        timestamp: streamMessage.getTimestamp(),
        sequenceNo: streamMessage.getSequenceNumber(),
        publisherId: streamMessage.getPublisherId(),
        msgChainId: streamMessage.getMsgChainId(),
        previousTimestamp: streamMessage.prevMsgRef ? streamMessage.prevMsgRef.timestamp : null,
        previousSequenceNo: streamMessage.prevMsgRef ? streamMessage.prevMsgRef.sequenceNumber : null,
        data: streamMessage.getParsedContent(),
        signature: streamMessage.signature,
        signatureType: streamMessage.signatureType,
    }
}

class Storage {
    constructor(cassandraClient, useTtl, isBatching = true) {
        this.cassandraClient = cassandraClient
        const insertStatement = useTtl ? INSERT_STATEMENT_WITH_TTL : INSERT_STATEMENT
        if (isBatching) {
            this.storeStrategy = batchingStore(cassandraClient, insertStatement)
        } else {
            this.storeStrategy = individualStore(cassandraClient, insertStatement)
        }
    }

    store(streamMessage) {
        return this.storeStrategy.store(streamMessage)
    }

    requestLast(streamId, streamPartition, n) {
        if (!Number.isInteger(n)) {
            throw new Error('n is not an integer')
        }
        const query = 'SELECT * FROM stream_data '
            + 'WHERE id = $1 AND partition = $2 '
            + 'ORDER BY ts DESC, sequence_no DESC '
            + 'LIMIT $3'
        const queryParams = [streamId, streamPartition, n]

        // Wrap as stream for consistency with other fetch functions
        const readableStream = new Readable({
            objectMode: true,
            read() {},
        })

        this.cassandraClient.query(query, queryParams, (err, res) => {
            if (err) {
                readableStream.emit('error', err)
            } else {
                res.rows.reverse().forEach((r) => readableStream.push(parseRow(r)))
                readableStream.push(null)
            }
        })

        return readableStream
    }

    requestFrom(streamId, streamPartition, fromTimestamp, fromSequenceNo, publisherId, msgChainId) {
        if (!Number.isInteger(fromTimestamp)) {
            throw new Error('fromTimestamp is not an integer')
        }
        if (fromSequenceNo != null && !Number.isInteger(fromSequenceNo)) {
            throw new Error('fromSequenceNo is not an integer')
        }

        if (fromSequenceNo != null && publisherId != null && msgChainId != null) {
            return this._fetchFromMessageRefForPublisher(streamId, streamPartition, fromTimestamp,
                fromSequenceNo, publisherId, msgChainId)
        }
        if ((fromSequenceNo == null || fromSequenceNo === 0) && publisherId == null && msgChainId == null) {
            return this._fetchFromTimestamp(streamId, streamPartition, fromTimestamp)
        }

        throw new Error('Invalid combination of requestFrom arguments')
    }

    _fetchFromTimestamp(streamId, streamPartition, from) {
        if (!Number.isInteger(from)) {
            throw new Error('from is not an integer')
        }

        const query = 'SELECT * FROM stream_data WHERE id = ? AND partition = ? AND ts >= ? ORDER BY ts ASC, sequence_no ASC'
        const queryParams = [streamId, streamPartition, from]
        return this._queryWithStreamingResults(query, queryParams)
    }

    _fetchFromMessageRefForPublisher(streamId, streamPartition, fromTimestamp, fromSequenceNo, publisherId, msgChainId) {
        // Cassandra doesn't allow ORs in WHERE clause so we need to do 2 queries.
        // Once a range (id/partition/ts/sequence_no) has been selected in Cassandra, filtering it by publisher_id requires to ALLOW FILTERING.
        const query1 = 'SELECT * FROM stream_data WHERE id = ? AND partition = ? AND ts = ? AND sequence_no >= ? AND publisher_id = ? '
            + 'AND msg_chain_id = ? ORDER BY ts ASC, sequence_no ASC ALLOW FILTERING'
        const query2 = 'SELECT * FROM stream_data WHERE id = ? AND partition = ? AND ts > ? AND publisher_id = ? '
            + 'AND msg_chain_id = ? ORDER BY ts ASC, sequence_no ASC ALLOW FILTERING'
        const queryParams1 = [streamId, streamPartition, fromTimestamp, fromSequenceNo, publisherId, msgChainId]
        const queryParams2 = [streamId, streamPartition, fromTimestamp, publisherId, msgChainId]
        const stream1 = this._queryWithStreamingResults(query1, queryParams1)
        const stream2 = this._queryWithStreamingResults(query2, queryParams2)
        return merge2(stream1, stream2)
    }

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
            return this._fetchBetweenMessageRefsForPublisher(streamId, streamPartition, fromTimestamp,
                fromSequenceNo, toTimestamp, toSequenceNo, publisherId, msgChainId)
        }
        if ((fromSequenceNo == null || fromSequenceNo === 0) && (toSequenceNo == null || toSequenceNo === 0)
            && publisherId == null && msgChainId == null) {
            return this._fetchBetweenTimestamps(streamId, streamPartition, fromTimestamp, toTimestamp)
        }

        throw new Error('Invalid combination of requestFrom arguments')
    }

    _fetchBetweenTimestamps(streamId, streamPartition, from, to) {
        if (!Number.isInteger(from)) {
            throw new Error('from is not an integer')
        }

        if (!Number.isInteger(to)) {
            throw new Error('to is not an integer')
        }

        const query = 'SELECT * FROM stream_data WHERE id = ? AND partition = ? AND ts >= ? AND ts <= ? ORDER BY ts ASC, sequence_no ASC'
        const queryParams = [streamId, streamPartition, from, to]
        return this._queryWithStreamingResults(query, queryParams)
    }

    _fetchBetweenMessageRefsForPublisher(
        streamId,
        streamPartition,
        fromTimestamp,
        fromSequenceNo,
        toTimestamp,
        toSequenceNo,
        publisherId,
        msgChainId
    ) {
        // Cassandra doesn't allow ORs in WHERE clause so we need to do 3 queries.
        // Once a range (id/partition/ts/sequence_no) has been selected in Cassandra, filtering it by publisher_id requires to ALLOW FILTERING.
        const query1 = 'SELECT * FROM stream_data WHERE id = ? AND partition = ? AND ts = ? AND sequence_no >= ? AND publisher_id = ? '
            + 'AND msg_chain_id = ? ORDER BY ts ASC, sequence_no ASC ALLOW FILTERING'
        const query2 = 'SELECT * FROM stream_data WHERE id = ? AND partition = ? AND ts > ? AND ts < ? AND publisher_id = ? '
            + 'AND msg_chain_id = ? ORDER BY ts ASC, sequence_no ASC ALLOW FILTERING'
        const query3 = 'SELECT * FROM stream_data WHERE id = ? AND partition = ? AND ts = ? AND sequence_no <= ? AND publisher_id = ? '
            + 'AND msg_chain_id = ? ORDER BY ts ASC, sequence_no ASC ALLOW FILTERING'
        const queryParams1 = [streamId, streamPartition, fromTimestamp, fromSequenceNo, publisherId, msgChainId]
        const queryParams2 = [streamId, streamPartition, fromTimestamp, toTimestamp, publisherId, msgChainId]
        const queryParams3 = [streamId, streamPartition, toTimestamp, toSequenceNo, publisherId, msgChainId]
        const stream1 = this._queryWithStreamingResults(query1, queryParams1)
        const stream2 = this._queryWithStreamingResults(query2, queryParams2)
        const stream3 = this._queryWithStreamingResults(query3, queryParams3)
        return merge2(stream1, stream2, stream3)
    }

    close() {
        this.storeStrategy.close()
        return this.cassandraClient.shutdown()
    }

    _queryWithStreamingResults(query, queryParams) {
        return this.cassandraClient.stream(query, queryParams, {
            prepare: true,
            autoPage: true,
        }).pipe(new Transform({
            objectMode: true,
            transform: (row, _, done) => {
                done(null, parseRow(row))
            },
        }))
    }
}

const startTimescaleStorage = async ({
    user,
    password,
    host,
    database,
    port,
    useTtl = false,
    isBatching = false
}) => {
    const pool = new Pool({
        user,
        password,
        host,
        database,
        port
    })

    try {
        const client = await pool.connect()
        return new Storage(client, useTtl, isBatching)
    } catch (err) {
        console.error('Timescalse not responding yet...')
    }

    throw new Error('Failed to connect to TimescaleDb')
}

module.exports = {
    Storage,
    startTimescaleStorage,
}
