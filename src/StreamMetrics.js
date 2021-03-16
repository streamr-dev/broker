const logger = require('./helpers/logger')('streamr:broker')

const throttledAvg = (avg, avgInterval) => {
    return (0.8 * avg) + (0.2 * avgInterval)
}

class StoppedError extends Error {
    constructor(errorText) {
        super(errorText)
        this.code = 'StoppedError'
        Error.captureStackTrace(this, StoppedError)
    }
}

class StreamMetrics {
    constructor(
        client,
        metricsContext,
        brokerAddress,
        interval, // sec/min/hour/day
    ) {
        this.stopped = false

        this.path = '/streamr/node/metrics/' + interval

        this.client = client
        this.metricsContext = metricsContext

        this.brokerAddress = brokerAddress
        this.interval = interval

        switch (this.interval) {
            case 'sec':
                this.reportMiliseconds = 1000
                break
            case 'min':
                this.sourcePath = '/streamr/node/metrics/sec'
                this.sourceInterval = 60
                this.reportMiliseconds = 60 * 1000
                break
            case 'hour':
                this.sourcePath = '/streamr/node/metrics/min'
                this.sourceInterval = 60
                this.reportMiliseconds = 60 * 60 * 1000

                break
            case 'day':
                this.sourcePath = '/streamr/node/metrics/hour'
                this.sourceInterval = 24
                this.reportMiliseconds = 24 * 60 * 60 * 1000
                break
            default:
                throw new Error('Unrecognized interval string, should be sec/min/hour/day')
        }

        this.report = {
            peerName: '',
            peerId: '',
            broker: {
                messagesToNetworkPerSec: 0,
                bytesToNetworkPerSec: 0,
                messagesFromNetworkPerSec: 0,
                bytesFromNetworkPerSec: 0,
            },
            network: {
                avgLatencyMs: 0,
                bytesToPeersPerSec: 0,
                bytesFromPeersPerSec: 0,
                connections: 0,
            },
            storage: {
                bytesWrittenPerSec: 0,
                bytesReadPerSec: 0,
            },

            startTime: 0,
            currentTime: 0,
            timestamp: 0
        }

        logger.info(`Started StreamMetrics for interval ${this.interval} running every ${this.reportMiliseconds / 1000}s`)
    }

    async createMetricsStream(path) {
        const metricsStream = await this.client.getOrCreateStream({
            name: `Metrics ${path} for broker ${this.brokerAddress}`,
            id: this.brokerAddress + path
        })

        await metricsStream.grantPermission('stream_get', null)
        await metricsStream.grantPermission('stream_subscribe', null)
        return metricsStream.id
    }

    async publishReport() {
        if (!this.stopped && this.client.connection.isConnectionValid()) {
            logger.info(`publishing ${this.report} to stream ${this.targetStreamId}`)
            return this.client.publish(this.targetStreamId, this.report)
        }
        return false
    }

    getResend(stream, last, timeout = 10 * 1000) {
        return new Promise((resolve, reject) => {
            let cummulative = 'hi:'
            logger.info('stopped:' + this.stopped)
            logger.info('isConnectionValid:' + this.client.connection.isConnectionValid())
            setInterval(() => {
                logger.info('isConnectionValid:' + this.client.connection.isConnectionValid())

            },500)
            if (this.stopped || !this.client.connection.isConnectionValid()) {
                return reject(new StoppedError('StreamMetrics stopped 2'))
            }

            const startTimeout = () => {
                return setTimeout(() => {
                    reject(new Error('StreamMetrics timed out'))
                }, timeout)
            }

            let timeoutId = startTimeout()
            const messages = []

            this.client.resend(
                {
                    stream,
                    resend: {
                        last
                    }
                },
                (message) => {
                    /*if (this.stopped || !this.client.connection.isConnectionValid()) {
                        reject(new StoppedError('StreamMetrics stopped'))
                    } else {*/
                        messages.push(message)
                        clearTimeout(timeoutId)
                        timeoutId = startTimeout()
                    //}
                }
            )
                .then((eventEmitter) => {
                    eventEmitter.once('resent', () => {
                        resolve(messages)
                    })

                    eventEmitter.once('no_resend', () => {
                        resolve(messages)
                    })
                })
                .catch(reject)
        })
    }

    stop() {
        this.stopped = true
        clearTimeout(this.metricsReportTimeout)
        logger.info(`Stopped StreamMetrics for ${this.interval}`)
    }

    async runReport() {
        try {
            const metricsReport = await this.metricsContext.report(true)
            if (this.stopped) {
                return
            }

            this.report.peerName = metricsReport.peerId
            this.report.peerId = metricsReport.peerName || metricsReport.peerId

            if (this.interval === 'sec') {
                if (this.report.timestamp === 0) {
                    // first iteration, assign values

                    this.report.broker.messagesToNetworkPerSec = metricsReport.metrics['broker/publisher'].messages.rate
                    this.report.broker.bytesToNetworkPerSec = metricsReport.metrics['broker/publisher'].bytes.rate
                    this.report.broker.messagesFromNetworkPerSec = 0
                    this.report.broker.bytesFromNetworkPerSec = 0

                    this.report.network.avgLatencyMs = metricsReport.metrics.node.latency.rate
                    this.report.network.bytesToPeersPerSec = metricsReport.metrics.WebRtcEndpoint.outSpeed.rate || 0
                    this.report.network.bytesFromPeersPerSec = metricsReport.metrics.WebRtcEndpoint.inSpeed.rate || 0
                    this.report.network.connections = metricsReport.metrics.WebRtcEndpoint.connections || 0

                    this.report.storage.bytesWrittenPerSec = (metricsReport.metrics['broker/cassandra']) ? metricsReport.metrics['broker/cassandra'].writeBytes : 0
                    this.report.storage.bytesReadPerSec = (metricsReport.metrics['broker/cassandra']) ? metricsReport.metrics['broker/cassandra'].readBytes : 0

                    this.report.startTime = metricsReport.startTime
                    this.report.currentTime = metricsReport.currentTime
                    this.report.timestamp = metricsReport.currentTime
                } else {
                    // calculate averaged values
                    this.report.broker.messagesToNetworkPerSec = throttledAvg(this.report.broker.messagesToNetworkPerSec, metricsReport.metrics['broker/publisher'].messages.rate)
                    this.report.broker.bytesToNetworkPerSec = throttledAvg(this.report.broker.bytesToNetworkPerSec, metricsReport.metrics['broker/publisher'].bytes.rate)

                    this.report.network.avgLatencyMs = throttledAvg(this.report.network.avgLatencyMs, metricsReport.metrics.node.latency.rate)
                    this.report.network.bytesToPeersPerSec = throttledAvg(this.report.network.bytesToPeersPerSec, metricsReport.metrics.WebRtcEndpoint.outSpeed.rate || 0)
                    this.report.network.bytesFromPeersPerSec = throttledAvg(this.report.network.bytesFromPeersPerSec, metricsReport.metrics.WebRtcEndpoint.inSpeed.rate || 0)
                    this.report.network.connections = throttledAvg(this.report.network.connections, metricsReport.metrics.WebRtcEndpoint.connections || 0)

                    if (metricsReport.metrics['broker/cassandra']) {
                        this.report.storage.bytesWrittenPerSec = throttledAvg(this.report.storage.bytesWrittenPerSec, (metricsReport.metrics['broker/cassandra']) ? metricsReport.metrics['broker/cassandra'].writeBytes : 0)
                        this.report.storage.bytesReadPerSec = throttledAvg(this.report.storage.bytesReadPerSec, (metricsReport.metrics['broker/cassandra']) ? metricsReport.metrics['broker/cassandra'].readBytes : 0)
                    }

                    this.report.currentTime = metricsReport.currentTime
                    this.report.timestamp = metricsReport.currentTime
                }

                await this.publishReport()
            } else {
                const now = Date.now()
                const messages = await this.getResend(this.sourceStreamId, this.sourceInterval)
                console.log(messages)
                logger.info(`found ${messages.length} messages`)
                logger.info(messages)
                if (messages.length === 0) {
                    await this.publishReport()
                    logger.info('CALLED PUBLISH REPORT FOR EMPTY MESSAGE QUEUE')
                } else {
                    for (let i = 0; i < messages.length; i++) {
                        this.report.broker.messagesToNetworkPerSec += messages[i].broker.messagesToNetworkPerSec
                        this.report.broker.bytesToNetworkPerSec += messages[i].broker.bytesToNetworkPerSec
                        this.report.network.avgLatencyMs += messages[i].network.avgLatencyMs

                        this.report.broker.messagesToNetworkPerSec += messages[i].broker.messagesToNetworkPerSec
                        this.report.broker.bytesToNetworkPerSec += messages[i].broker.bytesToNetworkPerSec

                        this.report.network.avgLatencyMs += messages[i].network.avgLatencyMs
                        this.report.network.bytesToPeersPerSec += messages[i].network.bytesToPeersPerSec
                        this.report.network.bytesFromPeersPerSec += messages[i].network.bytesFromPeersPerSec
                        this.report.network.connections += messages[i].network.connections

                        if (metricsReport.metrics['broker/cassandra']) {
                            this.report.storage.bytesWrittenPerSec += messages[i].storage.bytesWrittenPerSec
                            this.report.storage.bytesReadPerSec += messages[i].storage.bytesReadPerSec
                        }
                    }

                    if (messages.length > 0) {
                        this.report.broker.messagesToNetworkPerSec /= messages.length
                        this.report.broker.bytesToNetworkPerSec /= messages.length
                        this.report.network.avgLatencyMs /= messages.length

                        this.report.broker.messagesToNetworkPerSec /= messages.length
                        this.report.broker.bytesToNetworkPerSec /= messages.length

                        this.report.network.avgLatencyMs /= messages.length
                        this.report.network.bytesToPeersPerSec /= messages.length
                        this.report.network.bytesFromPeersPerSec /= messages.length
                        this.report.network.connections /= messages.length

                        if (metricsReport.metrics['broker/cassandra']) {
                            this.report.storage.bytesWrittenPerSec /= messages.length
                            this.report.storage.bytesReadPerSec /= messages.length
                        }

                        const lastMessage = messages[messages.length - 1]
                        if ((lastMessage.timestamp + this.reportMiliseconds - now) < 0) {
                            await this.publishReport()
                        }
                    }
                }
            }
        } catch (e) {
            if (e.code === 'StoppedError') {
                console.log(e)
            } else {
                logger.warn(e)
            }
        }

        if (!this.stopped) {
            this.metricsReportTimeout = setTimeout(async () => {
                await this.runReport()
            }, this.reportMiliseconds)
        }
    }
}

const waitForClient = (client, timeoutInSeconds = 3) => {
    return new Promise((resolve, reject) => {
        setTimeout(() => {
            reject(`Waiting for client connection in StreamMetrics timed out after ${timeoutInSeconds}s`)
        }, timeoutInSeconds * 1000)
        client.once('error', (err) => {
            logger.error(err)
            reject(err)
        })

        client.once('connected', () => {
            logger.info('client connected')
            resolve()
        })
    })
}

module.exports = async function startMetrics(client, metricsContext, brokerAddress, interval) {
    
    if (!client.connection.isConnectionValid()){
        await waitForClient(client)
    }
    
    const metrics = new StreamMetrics(client, metricsContext, brokerAddress, interval)
    metrics.targetStreamId = await metrics.createMetricsStream(metrics.path)

    if (metrics.sourcePath) {
        metrics.sourceStreamId = await metrics.createMetricsStream(metrics.sourcePath)
    }

    metrics.runReport()
    return metrics
}
