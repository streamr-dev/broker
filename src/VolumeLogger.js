const StreamrClient = require('streamr-client')

module.exports = class VolumeLogger {
    constructor(reportingIntervalSeconds = 60, networkNode = undefined, client = undefined, streamId = undefined) {
        this.reportingIntervalSeconds = reportingIntervalSeconds
        this.connectionCountMQTT = 0
        this.connectionCountWS = 0
        this.inCount = 0
        this.inBytes = 0
        this.outCount = 0
        this.outBytes = 0
        this.totalBufferSize = 0
        this.lastVolumeStatistics = {}
        this.client = client
        this.streamId = streamId
        this.networkNode = networkNode

        if (this.reportingIntervalSeconds > 0) {
            this.interval = setInterval(async () => {
                await this.reportAndReset()
            }, this.reportingIntervalSeconds * 1000)
        }
    }

    logInput(bytes) {
        this.inCount += 1
        this.inBytes += bytes
    }

    logOutput(bytes) {
        this.outCount += 1
        this.outBytes += bytes
    }

    async reportAndReset() {
        const inPerSecond = this.inCount / this.reportingIntervalSeconds
        const outPerSecond = this.outCount / this.reportingIntervalSeconds
        const kbInPerSecond = (this.inBytes / this.reportingIntervalSeconds) / 1000
        const kbOutPerSecond = (this.outBytes / this.reportingIntervalSeconds) / 1000

        const networkMetrics = await this.networkNode.getMetrics()
        const connectionCount = this.connectionCountWS + this.connectionCountMQTT

        this.lastVolumeStatistics = {
            id: this.networkNode.opts.id,
            timestamp: Date.now(),
            totalBufferSize: this.totalBufferSize,
            connectionCount,
            connectionCountMQTT: this.connectionCountMQTT,
            connectionCountWS: this.connectionCountWS,
            input: {
                eventsPerSecond: Math.round(inPerSecond),
                kbPerSecond: Math.round(kbInPerSecond),
            },
            output: {
                eventsPerSecond: Math.round(outPerSecond),
                kbPerSecond: Math.round(kbOutPerSecond),
            },
        }

        console.log(
            'Connections: %d, Broker messages in/sec: %d, Broker messages out/sec: %d, '
            + 'Network messages in/sec: %d, Network messages out/sec: %d, '
            + 'Network IN bytes/second: %d, Network OUT bytes/second: %d',
            connectionCount,
            inPerSecond < 10 ? inPerSecond.toFixed(1) : Math.round(inPerSecond),
            outPerSecond < 10 ? outPerSecond.toFixed(1) : Math.round(outPerSecond),
            networkMetrics.mainMetrics.msgInSpeed,
            networkMetrics.mainMetrics.msgOutSpeed,
            networkMetrics.mainMetrics.inSpeed,
            networkMetrics.mainMetrics.outSpeed,
        )

        this.inCount = 0
        this.outCount = 0
        this.inBytes = 0
        this.outBytes = 0

        this._sendReport({
            broker: this.lastVolumeStatistics,
            network: networkMetrics
        })
    }

    _sendReport(data) {
        if (this.client instanceof StreamrClient && this.streamId !== undefined) {
            this.client.publishHttp(this.streamId, data)
        }
    }

    close() {
        console.log('VolumeLogger closing.')
        clearInterval(this.interval)
    }
}
