const MessageNotSignedError = require('./errors/MessageNotSignedError')
const VolumeLogger = require('./VolumeLogger')

module.exports = class Publisher {
    constructor(networkNode, volumeLogger = new VolumeLogger(0)) {
        this.networkNode = networkNode
        this.volumeLogger = volumeLogger
    }

    publish(stream, streamMessage) {
        if (stream.requireSignedData && !streamMessage.signature) {
            throw new MessageNotSignedError('This stream requires published data to be signed.')
        }

        this.volumeLogger.logInput(streamMessage.getContent().length)

        this.networkNode.publish(
            streamMessage.getStreamId(),
            streamMessage.getStreamPartition(),
            streamMessage.getTimestamp(),
            streamMessage.messageId.sequenceNumber,
            streamMessage.getPublisherId() || '',
            streamMessage.messageId.msgChainId || '',
            streamMessage.prevMsgRef == null ? null : streamMessage.prevMsgRef.timestamp,
            streamMessage.prevMsgRef == null ? null : streamMessage.prevMsgRef.sequenceNumber,
            streamMessage.parsedContent,
        )
    }
}
