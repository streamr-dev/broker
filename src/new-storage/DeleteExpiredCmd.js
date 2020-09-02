const cassandra = require('cassandra-driver')
const allSettled = require('promise.allsettled')
const fetch = require('node-fetch')
const pLimit = require('p-limit')

const validateConfig = require('../helpers/validateConfig')

class DeleteExpiredCmd {
    constructor(config) {
        validateConfig(config)

        this.baseUrl = config.streamrUrl

        const authProvider = new cassandra.auth.PlainTextAuthProvider(config.cassandraNew.username, config.cassandraNew.password)
        this.cassandraClient = new cassandra.Client({
            contactPoints: [...config.cassandraNew.hosts],
            localDataCenter: config.cassandraNew.datacenter,
            keyspace: config.cassandraNew.keyspace,
            authProvider,
            pooling: {
                maxRequestsPerConnection: 32768
            }
        })

        // used for limited concurrency
        this.limit = pLimit(5)
        // this.progressBar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic)
    }

    async _getStreams() {
        const result = []

        const query = 'SELECT DISTINCT stream_id, partition FROM bucket'
        const resultSet = await this.cassandraClient.execute(query)

        if (resultSet.rows.length) {
            resultSet.rows.forEach((row) => {
                result.push({
                    streamId: row.stream_id,
                    partition: row.partition
                })
            })
        }

        return result
    }

    async _fetchStreamsInfo(streams) {
        const tasks = streams.map((stream) => {
            return this.limit(async () => {
                const url = `${this.baseUrl}/api/v1/streams/${stream.streamId}/validation`
                return fetch(url).then((res) => res.json()).then((json) => {
                    return {
                        streamId: stream.streamId,
                        partition: stream.partition,
                        storageDays: json.storageDays
                    }
                }).catch((e) => console.error(e))
            })
        })

        return Promise.all(tasks)
    }

    async run() {
        try {
            const streams = await this._getStreams()
            console.info(`Found ${streams.length} unique streams`)

            const testingStreams = []
            testingStreams.push({
                streamId: 'd8NXKCAxToOBB0Our6yYkg',
                partition: 0
            })

            testingStreams.push({
                streamId: 'j3AHY-6GRoW7HDbMrCqm5g',
                partition: 0
            })

            const streamsInfo = await this._fetchStreamsInfo(testingStreams)
            console.log(streamsInfo)
            // this.progressBar.start(streams.length, 0)
        } catch (e) {
            console.error(e)
            process.exit(-1)
        } finally {
            // this.progressBar.stop()
            await this.cassandraClient.shutdown()
        }
    }
}

module.exports = DeleteExpiredCmd
