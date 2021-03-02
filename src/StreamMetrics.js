module.exports = class StreamMetrics {
    constructor(
        client, 
        peerId,
        interval, // sec/min/hour/day
    ){
        this.interval = interval
        this.client = client 

        this.report = {
            peerName: peerId,
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

        this.metricsReportTimeout = setTimeout(metricsReportFn, 1000)

    }

    async throtheledAvg (avg, avgInterval) {
        return 0.9 * avg + 0.2 * avgInterval
    }

    async securePublish (stream, data){
        if (this.stopped) {
            return
        }
        this.client.publish(stream, data)
    }

    async getResend (stream, last, timeout = 1000) {
        return new Promise((resolve, reject) => {
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
                        messages.push(message)
                        clearTimeout(timeoutId)
                        timeoutId = startTimeout()
                        if (messages.length === last) {
                            resolve(messages)
                        }
                    }
                )
            }
        })
    }


    async publishReport(preventPublish = false){
        try {
            sec += 1

            const metricsReport = await this.metricsContext.report()
            // console.log(metricsReport)
            // console.log(metricsReport.metrics)//.WsEndpoint.msgSpeed)

            const secReport = {
                // peerId: brokerAddress,
                peerName: metricsReport.peerId,
                broker: {
                    messagesToNetworkPerSec: metricsReport.metrics['broker/publisher'].messages.rate,
                    bytesToNetworkPerSec: metricsReport.metrics['broker/publisher'].bytes.rate,
                    messagesFromNetworkPerSec: 0,
                    bytesFromNetworkPerSec: 0,
                },
                network: {
                    avgLatencyMs: metricsReport.metrics.node.latency.rate,
                    bytesToPeersPerSec: metricsReport.metrics.WebRtcEndpoint.outSpeed.rate || 0,
                    bytesFromPeersPerSec: metricsReport.metrics.WebRtcEndpoint.inSpeed.rate || 0,
                    connections: metricsReport.metrics.WebRtcEndpoint.connections || 0
                },
                storage: {
                    bytesWrittenPerSec: (metricsReport.metrics['broker/cassandra']) ? metricsReport.metrics['broker/cassandra'].writeBytes : 0,
                    bytesReadPerSec: (metricsReport.metrics['broker/cassandra']) ? metricsReport.metrics['broker/cassandra'].readBytes : 0,
                },

                startTime: metricsReport.startTime,
                currentTime: metricsReport.currentTime,
                timestamp: metricsReport.currentTime,

            }

            const hourReport = {
                // peerId: brokerAddress,
                peerName: metricsReport.peerId,
                broker: {
                    messagesToNetworkPerSec: metricsReport.metrics['broker/publisher'].messages.rate,
                    bytesToNetworkPerSec: metricsReport.metrics['broker/publisher'].bytes.rate,
                    messagesFromNetworkPerSec: 0,
                    bytesFromNetworkPerSec: 0,
                },
                network: {
                    avgLatencyMs: metricsReport.metrics.node.latency.rate,
                    bytesToPeersPerSec: metricsReport.metrics.WebRtcEndpoint.outSpeed.rate || 0,
                    bytesFromPeersPerSec: metricsReport.metrics.WebRtcEndpoint.inSpeed.rate || 0,
                    connections: metricsReport.metrics.WebRtcEndpoint.connections || 0
                },
                storage: {
                    bytesWrittenPerSec: (metricsReport.metrics['broker/cassandra']) ? metricsReport.metrics['broker/cassandra'].writeBytes : 0,
                    bytesReadPerSec: (metricsReport.metrics['broker/cassandra']) ? metricsReport.metrics['broker/cassandra'].readBytes : 0,
                },

                startTime: metricsReport.startTime,
                currentTime: metricsReport.currentTime,
                timestamp: metricsReport.currentTime,
            }

            const dayReport = {
                // peerId: brokerAddress,
                peerName: metricsReport.peerId,
                broker: {
                    messagesToNetworkPerSec: metricsReport.metrics['broker/publisher'].messages.rate,
                    bytesToNetworkPerSec: metricsReport.metrics['broker/publisher'].bytes.rate,
                    messagesFromNetworkPerSec: 0,
                    bytesFromNetworkPerSec: 0,
                },
                network: {
                    avgLatencyMs: metricsReport.metrics.node.latency.rate,
                    bytesToPeersPerSec: metricsReport.metrics.WebRtcEndpoint.outSpeed.rate || 0,
                    bytesFromPeersPerSec: metricsReport.metrics.WebRtcEndpoint.inSpeed.rate || 0,
                    connections: metricsReport.metrics.WebRtcEndpoint.connections || 0
                },
                storage: {
                    bytesWrittenPerSec: (metricsReport.metrics['broker/cassandra']) ? metricsReport.metrics['broker/cassandra'].writeBytes : 0,
                    bytesReadPerSec: (metricsReport.metrics['broker/cassandra']) ? metricsReport.metrics['broker/cassandra'].readBytes : 0,
                },

                startTime: metricsReport.startTime,
                currentTime: metricsReport.currentTime,
                timestamp: metricsReport.currentTime,
            }

            if (sec === 1) {
                minReport.peerName = secReport.peerName
                minReport.startTime = secReport.startTime
                minReport.currentTime = secReport.currentTime
                minReport.timestamp = 0

                minReport.broker.messagesToNetworkPerSec = secReport.broker.messagesToNetworkPerSec
                minReport.broker.bytesToNetworkPerSec = secReport.broker.bytesToNetworkPerSec

                minReport.network.avgLatencyMs = secReport.network.avgLatencyMs
                minReport.network.bytesToPeersPerSec = secReport.network.bytesToPeersPerSec
                minReport.network.bytesFromPeersPerSec = secReport.network.bytesFromPeersPerSec
                minReport.network.connections = secReport.network.connections

                if (metricsReport.metrics['broker/cassandra']) {
                    minReport.storage.bytesWrittenPerSec = secReport.storage.bytesWrittenPerSec
                    minReport.storage.bytesReadPerSec = secReport.storage.bytesReadPerSec
                }
            } else {
                minReport.broker.messagesToNetworkPerSec = throtheledAvg(minReport.broker.messagesToNetworkPerSec, secReport.broker.messagesToNetworkPerSec)
                minReport.broker.bytesToNetworkPerSec = throtheledAvg(minReport.broker.bytesToNetworkPerSec, secReport.broker.bytesToNetworkPerSec)

                minReport.network.avgLatencyMs = throtheledAvg(minReport.network.avgLatencyMs, secReport.network.avgLatencyMs)
                minReport.network.bytesToPeersPerSec = throtheledAvg(minReport.network.bytesToPeersPerSec, secReport.network.bytesToPeersPerSec)
                minReport.network.bytesFromPeersPerSec = throtheledAvg(minReport.network.bytesFromPeersPerSec, secReport.network.bytesFromPeersPerSec)
                minReport.network.connections = throtheledAvg(minReport.network.connections, secReport.network.connections)

                if (metricsReport.metrics['broker/cassandra']) {
                    minReport.storage.bytesWrittenPerSec = throtheledAvg(minReport.storage.bytesWrittenPerSec, secReport.storage.bytesWrittenPerSec)
                    minReport.storage.bytesReadPerSec = throtheledAvg(minReport.storage.bytesReadPerSec, secReport.storage.bytesReadPerSec)
                }
            }

            securePublish(
                this.streamIds.secStreamId,
                secReport
            )

            const now = Date.now()

            if (sec === 60 || (minReport.timestamp + (60 * 1000) - now) < 0) {
                minReport.timestamp = secReport.currentTime
                sec = 0

                securePublish(
                    this.streamIds.minStreamId,
                    minReport
                )

                // let's check, once a minute, if it's time to run hour and/or daily reports

                // get the last msg to check if it's been an hour

                const lastHourReports = await getResend(this.streamIds.hourStreamId, 1)

                if (lastHourReports.length === 0) {
                    securePublish(
                        this.streamIds.hourStreamId,
                        hourReport
                    )
                } else if ((lastHourReports[0].timestamp + (60 * 60 * 1000) - now) < 0) {
                    // fetch the last 60 minute reports and get the averages
                    const messages = await getResend(this.streamIds.minuteStreamId, 60)

                    for (let i = 1; i < messages.length; i++) {
                        hourReport.broker.messagesToNetworkPerSec = throtheledAvg(hourReport.broker.messagesToNetworkPerSec, messages[i].broker.messagesToNetworkPerSec)
                        hourReport.broker.bytesToNetworkPerSec = throtheledAvg(hourReport.broker.bytesToNetworkPerSec, messages[i].broker.bytesToNetworkPerSec)
                        hourReport.network.avgLatencyMs = throtheledAvg(hourReport.network.avgLatencyMs, messages[i].network.avgLatencyMs)

                        hourReport.broker.messagesToNetworkPerSec = throtheledAvg(hourReport.broker.messagesToNetworkPerSec, messages[i].broker.messagesToNetworkPerSec)
                        hourReport.broker.bytesToNetworkPerSec = throtheledAvg(hourReport.broker.bytesToNetworkPerSec, messages[i].broker.bytesToNetworkPerSec)

                        hourReport.network.avgLatencyMs = throtheledAvg(hourReport.network.avgLatencyMs, messages[i].network.avgLatencyMs)
                        hourReport.network.bytesToPeersPerSec = throtheledAvg(hourReport.network.bytesToPeersPerSec, messages[i].network.bytesToPeersPerSec)
                        hourReport.network.bytesFromPeersPerSec = throtheledAvg(hourReport.network.bytesFromPeersPerSec, messages[i].network.bytesFromPeersPerSec)
                        hourReport.network.connections = throtheledAvg(hourReport.network.connections, messages[i].network.connections)

                        if (metricsReport.metrics['broker/cassandra']) {
                            hourReport.storage.bytesWrittenPerSec = throtheledAvg(hourReport.storage.bytesWrittenPerSec, messages[i].storage.bytesWrittenPerSec)
                            hourReport.storage.bytesReadPerSec = throtheledAvg(hourReport.storage.bytesReadPerSec, messages[i].storage.bytesReadPerSec)
                        }
                    }

                    securePublish(
                        this.streamIds.hourStreamId,
                        hourReport
                    )
                }

                // do the same to inspect if a daily report is to be pushed
                const lastDayReports = await getResend(this.streamIds.dayStreamId, 1)
                if (lastDayReports.length === 0) {
                    securePublish(
                        this.streamIds.dayStreamId,
                        dayReport
                    )
                } else if ((lastDayReports[0].timestamp + (24 * 60 * 60 * 1000) - now) < 0) {
                    // fetch the last 60 minute reports and get the averages
                    const messages = await getResend(this.streamIds.hourStreamId, 24)

                    for (let i = 1; i < messages.length; i++) {
                        dayReport.broker.messagesToNetworkPerSec = throtheledAvg(dayReport.broker.messagesToNetworkPerSec, messages[i].broker.messagesToNetworkPerSec)
                        dayReport.broker.bytesToNetworkPerSec = throtheledAvg(dayReport.broker.bytesToNetworkPerSec, messages[i].broker.bytesToNetworkPerSec)

                        dayReport.network.avgLatencyMs = throtheledAvg(dayReport.network.avgLatencyMs, messages[i].network.avgLatencyMs)
                        dayReport.network.bytesToPeersPerSec = throtheledAvg(dayReport.network.bytesToPeersPerSec, messages[i].network.bytesToPeersPerSec)
                        dayReport.network.bytesFromPeersPerSec = throtheledAvg(dayReport.network.bytesFromPeersPerSec, messages[i].network.bytesFromPeersPerSec)
                        dayReport.network.connections = throtheledAvg(dayReport.network.connections, messages[i].network.connections)

                        if (metricsReport.metrics['broker/cassandra']) {
                            dayReport.storage.bytesWrittenPerSec = throtheledAvg(dayReport.storage.bytesWrittenPerSec, messages[i].storage.bytesWrittenPerSec)
                            dayReport.storage.bytesReadPerSec = throtheledAvg(dayReport.storage.bytesReadPerSec, messages[i].storage.bytesReadPerSec)
                        }
                    }

                    securePublish(
                        this.streamIds.dayStreamId,
                        dayReport
                    )
                }
            }
        } catch (e) {
            logger.warn(`Error reporting metrics stream ${e}`)
        }

        this.metricsReportTimeout = setTimeout(metricsReportFn, 1000)
    }
}