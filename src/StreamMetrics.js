const logger = require('./helpers/logger')('streamr:broker')

const throttledAvg = (avg, avgInterval) => {
    return (0.8 * avg) + (0.2 * avgInterval)
}

module.exports = class StreamMetrics {
    constructor(
        client,
        metricsContext,
        brokerAddress,
        interval, // sec/min/hour/day
    ) {
        const self = this

        this.stopped = false
        this.started = false

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
                this.legacyPath = '/streamr/node/metrics/sec'
                this.legacyInterval = 60
                this.reportMiliseconds = 60 * 1000
                break
            case 'hour':
                this.legacyPath = '/streamr/node/metrics/min'
                this.legacyInterval = 60
                this.reportMiliseconds = 60 * 60 * 1000

                break
            case 'day':
                this.legacyPath = '/streamr/node/metrics/hour'
                this.legacyInterval = 24
                this.reportMiliseconds = 24 * 60 * 60 * 1000
                break
            default:
                this.reportMiliseconds = 0
        }

        logger.info(`Started StreamMetrics for interval ${this.interval} running every ${this.reportMiliseconds / 1000}s`)

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

        this.createMetricsStream()
            .then((streamId) => {
                self.streamId = streamId
                return self.createMetricsStream(self.legacyPath)
            })
            .then((legacyStreamId) => {
                self.legacyStreamId = legacyStreamId
                return self.runReport()
            })
            .then(() => {
                self.metricsReportTimeout = setTimeout(() => {
                    self.runReport()
                }, self.reportMiliseconds)
            })
            .catch((e) => {
                logger.error(e)
            })
    }

    async createMetricsStream(_path) {
        const path = _path || this.path
        const metricsStream = await this.client.getOrCreateStream({
            name: `Metrics ${path} for broker ${this.brokerAddress}`,
            id: this.brokerAddress + path
        })

        await metricsStream.grantPermission('stream_get', null)
        await metricsStream.grantPermission('stream_subscribe', null)
        return metricsStream.id
    }

    publishReport() {
        if (!this.stopped) {
            this.client.publish(this.streamId, this.report)
        }
    }

    getResend(stream, last, timeout = 1000) {
        return new Promise((resolve, reject) => {
            if (this.stopped) {
                resolve([])
            }
            const startTimeout = () => {
                return setTimeout(() => {
                    resolve([])
                }, timeout)
            }

            let timeoutId = startTimeout()
            const messages = []
            if (this.stopped) {
                resolve(messages)
            } else {
                this.client.resend(
                    {
                        stream,
                        resend: {
                            last
                        }
                    },
                    (message) => {
                        if (this.stopped) {
                            resolve(messages)
                        } else {
                            messages.push(message)
                            clearTimeout(timeoutId)
                            timeoutId = startTimeout()
                        }
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
            }
        })
    }

    stop() {
        logger.info(`Stopped StreamMetrics for ${this.interval}`)
        this.stopped = true
    }

    async runReport(preventPublish = false) {
        try {
            if (this.stopped) {
                return
            }
            const metricsReport = await this.metricsContext.report(true)

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
                // calculations for min/hour/day
                if (this.report.timestamp === 0) {
                    // first iteration
                    const lastMessages = await this.getResend(this.legacyStreamId, 1)

                    if (lastMessages.length === 0) {
                        await this.publishReport()
                    } else {
                        const messages = await this.getResend(this.streamIds.minuteStreamId, this.legacyInterval)

                        for (let i = 1; i < messages.length; i++) {
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

                            if ((lastMessages[0].timestamp + this.reportMiliseconds - now) < 0) {
                                await this.publishReport()
                            }
                        }
                    }
                }
            }
        } catch (e) {
            logger.error(e)
        }

        const self = this
        this.metricsReportTimeout = setTimeout(() => {
            self.runReport()
        }, this.reportMiliseconds)
    }
}
