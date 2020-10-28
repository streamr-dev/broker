const VolumeLogger = require('./VolumeLogger')
const FailedToPublishError = require('./errors/FailedToPublishError')

const THRESHOLD_FOR_FUTURE_MESSAGES_IN_MS = 300 * 1000

const isTimestampTooFarInTheFuture = (timestamp) => {
    return timestamp > Date.now() + THRESHOLD_FOR_FUTURE_MESSAGES_IN_MS
}

module.exports = class Publisher {
    constructor(networkNode, streamMessageValidator, volumeLogger = new VolumeLogger(0)) {
        this.networkNode = networkNode
        this.streamMessageValidator = streamMessageValidator
        this.volumeLogger = volumeLogger

        if (!networkNode) {
            throw new Error('No networkNode defined!')
        }
        if (!streamMessageValidator) {
            throw new Error('No streamMessageValidator defined!')
        }
        if (!volumeLogger) {
            throw new Error('No volumeLogger defined!')
        }
    }

    async validateAndPublish(streamMessage) {
        if (isTimestampTooFarInTheFuture(streamMessage.getTimestamp())) {
            throw new FailedToPublishError(
                streamMessage.getStreamId(),
                `future timestamps are not allowed, max allowed +${THRESHOLD_FOR_FUTURE_MESSAGES_IN_MS} ms`
            )
        }

        // Only publish valid messages
        await this.streamMessageValidator.validate(streamMessage)

        // This throws if content not valid JSON
        streamMessage.getContent(true)

        this.volumeLogger.logInput(streamMessage.getContent(false).length)
        this.networkNode.publish(streamMessage)
    }
}
