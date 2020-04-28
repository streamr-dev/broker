const VolumeLogger = require('./VolumeLogger')

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
        // Only publish valid messages
        await this.streamMessageValidator.validate(streamMessage)
        this.volumeLogger.logInput(streamMessage.getContent().length)
        this.networkNode.publish(streamMessage)
    }
}
