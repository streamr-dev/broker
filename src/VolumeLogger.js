const io = require('@pm2/io')
const StreamrClient = require('streamr-client')

const logger = require('./helpers/logger')('streamr:VolumeLogger')

function formatNumber(n) {
    return n < 10 ? n.toFixed(1) : Math.round(n)
}

module.exports = class VolumeLogger {
    constructor(reportingIntervalSeconds = 60, metricsContext, client = undefined, streamIds = undefined) {
        this.metricsContext = metricsContext
        this.client = client
        this.streamIds = streamIds

        this.brokerConnectionCountMetric = io.metric({
            name: 'brokerConnectionCountMetric'
        })
        this.eventsInPerSecondMetric = io.metric({
            name: 'eventsIn/sec'
        })
        this.eventsOutPerSecondMetric = io.metric({
            name: 'eventsOut/sec'
        })
        this.kbInPerSecondMetric = io.metric({
            name: 'kbIn/sec'
        })
        this.kbOutPerSecondMetric = io.metric({
            name: 'kbOut/sec'
        })
        this.storageReadPerSecondMetric = io.metric({
            name: 'storageRead/sec'
        })
        this.storageWritePerSecondMetric = io.metric({
            name: 'storageWrite/sec'
        })
        this.storageReadKbPerSecondMetric = io.metric({
            name: 'storageReadKb/sec'
        })
        this.storageWriteKbPerSecondMetric = io.metric({
            name: 'storageWriteKb/sec'
        })
        this.totalBufferSizeMetric = io.metric({
            name: 'totalBufferSize'
        })
        this.ongoingResendsMetric = io.metric({
            name: 'ongoingResends'
        })
        this.meanResendAgeMetric = io.metric({
            name: 'meanResendAge'
        })
        this.totalBatchesMetric = io.metric({
            name: 'totalBatches'
        })
        this.meanBatchAge = io.metric({
            name: 'meanBatchAge'
        })
        this.messageQueueSizeMetric = io.metric({
            name: 'messageQueueSize'
        })

        if (this.client instanceof StreamrClient) {
            let sec = 0

            const throtheledAvg = (avg, avgInterval) => {
                return 0.9 * avg + 0.2 * avgInterval
            }

            const getResend = async (stream, last, timeout = 1000) => {
                return new Promise((resolve, reject) => {
                    const startTimeout = () => {
                        return setTimeout(() => {
                            resolve([])
                        }, timeout)
                    }

                    let timeoutId = startTimeout()
                    const messages = []
                    client.resend(
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
                })
            }

            const minReport = {
                peerId: '',
                broker: {
                    messagesToNetworkPerSec: -1,
                    bytesToNetworkPerSec: -1
                },
                network: {
                    avgLatencyMs: -1
                }
            }

            setInterval(async () => {
                sec += 1

                const metricsReport = await this.metricsContext.report()
                // console.log(metricsReport)
                // console.log(metricsReport.metrics)//.WsEndpoint.msgSpeed)

                const secReport = {
                    peerId: metricsReport.peerId,

                    startTime: metricsReport.startTime,
                    currentTime: metricsReport.currentTime,
                    timestamp: metricsReport.currentTime,

                    broker: {
                        messagesToNetworkPerSec: metricsReport.broker.messages.rate || metricsReport.publisher.messages.rate,
                        bytesToNetworkPerSec: metricsReport.broker.bytes.rate || metricsReport.publisher.bytes.rate
                    },
                    network: {
                        avgLatencyMs: metricsReport.node.latency.rate
                    }
                }

                const hourReport = {
                    peerId: metricsReport.peerId,

                    startTime: metricsReport.startTime,
                    currentTime: metricsReport.currentTime,
                    timestamp: metricsReport.currentTime,

                    broker: {
                        messagesToNetworkPerSec: metricsReport.broker.messages.rate || metricsReport.publisher.messages.rate,
                        bytesToNetworkPerSec: metricsReport.broker.bytes.rate || metricsReport.publisher.bytes.rate
                    },
                    network: {
                        avgLatencyMs: metricsReport.node.latency.rate
                    }
                }

                const dayReport = {
                    peerId: metricsReport.peerId,

                    startTime: metricsReport.startTime,
                    currentTime: metricsReport.currentTime,
                    timestamp: metricsReport.currentTime,

                    broker: {
                        messagesToNetworkPerSec: metricsReport.broker.messages.rate || metricsReport.publisher.messages.rate,
                        bytesToNetworkPerSec: metricsReport.broker.bytes.rate || metricsReport.publisher.bytes.rate
                    },
                    network: {
                        avgLatencyMs: metricsReport.node.latency.rate
                    }
                }

                if (sec === 1) {
                    minReport.peerId = secReport.peerId
                    minReport.startTime = secReport.startTime
                    minReport.currentTime = secReport.currentTime
                    minReport.timestamp = 0

                    minReport.broker.messagesToNetworkPerSec = secReport.broker.messagesToNetworkPerSec
                    minReport.broker.bytesToNetworkPerSec = secReport.broker.bytesToNetworkPerSec
                    minReport.network.avgLatencyMs = secReport.network.avgLatencyMs
                } else {
                    minReport.broker.messagesToNetworkPerSec = throtheledAvg(minReport.broker.messagesToNetworkPerSec, secReport.broker.messagesToNetworkPerSec)
                    minReport.broker.bytesToNetworkPerSec = throtheledAvg(minReport.broker.bytesToNetworkPerSec, secReport.broker.bytesToNetworkPerSec)
                    minReport.network.avgLatencyMs = throtheledAvg(minReport.network.avgLatencyMs, secReport.network.avgLatencyMs)
                }

                this.client.publish(
                    this.streamIds.secStreamId,
                    secReport
                )

                const now = Date.now()

                if (sec === 60 || (minReport.timestamp + (60 * 1000) - now) < 0) {
                    minReport.timestamp = secReport.currentTime
                    sec = 0
                    this.client.publish(
                        this.streamIds.minStreamId,
                        minReport
                    )

                    // let's check, once a minute, if it's time to run hour and/or daily reports

                    // get the last msg to check if it's been an hour

                    const lastHourReports = await getResend(this.streamIds.hourStreamId, 1)

                    if (lastHourReports.length === 0) {
                        this.client.publish(
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
                        }

                        this.client.publish(
                            this.streamIds.hourStreamId,
                            hourReport
                        )
                    }

                    // do the same to inspect if a daily report is to be pushed
                    const lastDayReports = await getResend(this.streamIds.dayStreamId, 1)
                    if (lastDayReports.length === 0) {
                        this.client.publish(
                            this.streamIds.hourStreamId,
                            dayReport
                        )
                    } else if ((lastDayReports[0].timestamp + (24 * 60 * 60 * 1000) - now) < 0) {
                        // fetch the last 60 minute reports and get the averages
                        const messages = await getResend(this.streamIds.hourStreamId, 24)

                        for (let i = 1; i < messages.length; i++) {
                            dayReport.broker.messagesToNetworkPerSec = throtheledAvg(dayReport.broker.messagesToNetworkPerSec, messages[i].broker.messagesToNetworkPerSec)
                            dayReport.broker.bytesToNetworkPerSec = throtheledAvg(dayReport.broker.bytesToNetworkPerSec, messages[i].broker.bytesToNetworkPerSec)
                            dayReport.network.avgLatencyMs = throtheledAvg(dayReport.network.avgLatencyMs, messages[i].network.avgLatencyMs)
                        }

                        this.client.publish(
                            this.streamIds.dayStreamId,
                            dayReport
                        )
                    }
                }
            }, 1000)
        }

        if (reportingIntervalSeconds > 0) {
            logger.info('starting legacy metrics reporting interval')
            const reportingIntervalInMs = reportingIntervalSeconds * 1000
            const reportFn = async () => {
                try {
                    await this.reportAndReset()
                } catch (e) {
                    logger.warn(`Error reporting metrics ${e}`)
                }
                this.timeout = setTimeout(reportFn, reportingIntervalInMs)
            }
            this.timeout = setTimeout(reportFn, reportingIntervalInMs)
        }
    }

    async reportAndReset() {
        const report = await this.metricsContext.report(true)

        // Report metrics to Streamr stream
        if (this.client instanceof StreamrClient && this.streamIds !== undefined && this.streamIds.metricsStreamId !== undefined) {
            this.client.publish(this.streamIds.metricsStreamId, report).catch((e) => {
                logger.warn(`failed to publish metrics to ${this.streamIds.metricsStreamId} because ${e}`)
            })
        }

        const inPerSecond = report.metrics['broker/publisher'].messages.rate
        const kbInPerSecond = report.metrics['broker/publisher'].bytes.rate / 1000
        const outPerSecond = (report.metrics['broker/ws'] ? report.metrics['broker/ws'].outMessages.rate : 0)
            + (report.metrics['broker/mqtt'] ? report.metrics['broker/mqtt'].outMessages.rate : 0)
            + (report.metrics['broker/http'] ? report.metrics['broker/http'].outMessages.rate : 0)
        const kbOutPerSecond = ((report.metrics['broker/ws'] ? report.metrics['broker/ws'].outBytes.rate : 0)
            + (report.metrics['broker/mqtt'] ? report.metrics['broker/mqtt'].outBytes.rate : 0)
            + (report.metrics['broker/http'] ? report.metrics['broker/http'].outBytes.rate : 0)) / 1000

        let storageReadCountPerSecond = 0
        let storageWriteCountPerSecond = 0
        let storageReadKbPerSecond = 0
        let storageWriteKbPerSecond = 0
        let totalBatches = 0
        let meanBatchAge = 0
        if (report.metrics['broker/cassandra']) {
            storageReadCountPerSecond = report.metrics['broker/cassandra'].readCount.rate
            storageWriteCountPerSecond = report.metrics['broker/cassandra'].writeCount.rate
            storageReadKbPerSecond = report.metrics['broker/cassandra'].readBytes.rate / 1000
            storageWriteKbPerSecond = report.metrics['broker/cassandra'].writeBytes.rate / 1000
            totalBatches = report.metrics['broker/cassandra'].batchManager.totalBatches
            meanBatchAge = report.metrics['broker/cassandra'].batchManager.meanBatchAge
        }

        const brokerConnectionCount = (report.metrics['broker/ws'] ? report.metrics['broker/ws'].connections : 0)
            + (report.metrics['broker/mqtt'] ? report.metrics['broker/mqtt'].connections : 0)

        const networkConnectionCount = report.metrics.WebRtcEndpoint.connections
        const networkInPerSecond = report.metrics.WebRtcEndpoint.msgInSpeed.rate
        const networkOutPerSecond = report.metrics.WebRtcEndpoint.msgOutSpeed.rate
        const networkKbInPerSecond = report.metrics.WebRtcEndpoint.inSpeed.rate / 1000
        const networkKbOutPerSecond = report.metrics.WebRtcEndpoint.outSpeed.rate / 1000
        const { messageQueueSize } = report.metrics.WebRtcEndpoint

        const ongoingResends = report.metrics.resends.numOfOngoingResends
        const resendMeanAge = report.metrics.resends.meanAge

        const totalBuffer = report.metrics.WebRtcEndpoint.totalWebSocketBuffer
            + (report.metrics['broker/ws'] ? report.metrics['broker/ws'].totalWebSocketBuffer : 0)

        logger.info(
            'Report\n'
            + '\tBroker connections: %d\n'
            + '\tBroker in: %d events/s, %d kb/s\n'
            + '\tBroker out: %d events/s, %d kb/s\n'
            + '\tNetwork connections %d\n'
            + '\tQueued messages: %d\n'
            + '\tNetwork in: %d events/s, %d kb/s\n'
            + '\tNetwork out: %d events/s, %d kb/s\n'
            + '\tStorage read: %d events/s, %d kb/s\n'
            + '\tStorage write: %d events/s, %d kb/s\n'
            + '\tTotal ongoing resends: %d (mean age %d ms)\n'
            + '\tTotal batches: %d (mean age %d ms)\n',
            brokerConnectionCount,
            formatNumber(inPerSecond),
            formatNumber(kbInPerSecond),
            formatNumber(outPerSecond),
            formatNumber(kbOutPerSecond),
            networkConnectionCount,
            messageQueueSize,
            formatNumber(networkInPerSecond),
            formatNumber(networkKbInPerSecond),
            formatNumber(networkOutPerSecond),
            formatNumber(networkKbOutPerSecond),
            formatNumber(storageReadCountPerSecond),
            formatNumber(storageReadKbPerSecond),
            formatNumber(storageWriteCountPerSecond),
            formatNumber(storageWriteKbPerSecond),
            ongoingResends,
            resendMeanAge,
            totalBatches,
            meanBatchAge
        )

        this.eventsInPerSecondMetric.set(inPerSecond)
        this.kbInPerSecondMetric.set(kbInPerSecond)
        this.eventsOutPerSecondMetric.set(outPerSecond)
        this.kbOutPerSecondMetric.set(kbOutPerSecond)
        this.storageReadPerSecondMetric.set(storageReadCountPerSecond)
        this.storageWritePerSecondMetric.set(storageWriteCountPerSecond)
        this.storageReadKbPerSecondMetric.set(storageReadKbPerSecond)
        this.storageWriteKbPerSecondMetric.set(storageWriteKbPerSecond)
        this.brokerConnectionCountMetric.set(brokerConnectionCount)
        this.totalBufferSizeMetric.set(totalBuffer)
        this.ongoingResendsMetric.set(ongoingResends)
        this.meanResendAgeMetric.set(resendMeanAge)
        this.messageQueueSizeMetric.set(messageQueueSize)
        if (report.metrics['broker/cassandra']) {
            this.totalBatchesMetric.set(totalBatches)
            this.meanBatchAge.set(meanBatchAge)
        }
    }

    close() {
        io.destroy()
        clearTimeout(this.timeout)
        if (this.client) {
            this.client.ensureDisconnected()
        }
    }
}
