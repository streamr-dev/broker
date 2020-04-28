module.exports = class FieldDetector {
    constructor(streamFetcher) {
        this.streamFetcher = streamFetcher
        this.configuredStreamIds = new Set()
    }

    async detectAndSetFields(streamMessage, apiKey, sessionToken) {
        const stream = await this.streamFetcher.fetch(streamMessage.getStreamId(), apiKey, sessionToken)

        if (this._shouldDetectAndSet(stream)) {
            this.configuredStreamIds.add(stream.id)

            const content = streamMessage.getParsedContent()
            const fields = []

            Object.keys(content).forEach((key) => {
                let type
                if (Array.isArray(content[key])) {
                    type = 'list'
                } else if ((typeof content[key]) === 'object') {
                    type = 'map'
                } else {
                    type = typeof content[key]
                }
                fields.push({
                    name: key,
                    type,
                })
            })
            try {
                await this.streamFetcher.setFields(stream.id, fields, apiKey, sessionToken)
            } catch (e) {
                this.configuredStreamIds.delete(stream.id)
                throw e
            }
        }
    }

    _shouldDetectAndSet(stream) {
        return stream.autoConfigure
            && (!stream.config || !stream.config.fields || stream.config.fields.length === 0)
            && !this.configuredStreamIds.has(stream.id)
    }
}
