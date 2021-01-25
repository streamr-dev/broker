const fetch = require('node-fetch')

/*
 * Connects to Core API and queries the configuration there.
 * Refreshes the config at regular intervals.
 */
const getStreamFromKey = (key) => {
    const [id, partitionStr] = key.split('::')
    return {
        id,
        partition: Number(partitionStr)
    }
}

const getKeyFromStream = (streamId, streamPartition) => {
    return `${streamId}::${streamPartition}`
}

module.exports = class StorageConfig {
    // use createInstance method instead: it fetches the up-to-date config from API
    constructor(nodeId, apiUrl) {
        this.streams = new Set()
        this.listeners = []
        this.nodeId = nodeId
        this.apiUrl = apiUrl
        this._poller = undefined
    }

    static async createInstance(nodeId, apiUrl, pollInterval = 10 * 60 * 60 * 1000) {
        const instance = new StorageConfig(nodeId, apiUrl)
        await instance.refresh()
        // eslint-disable-next-line require-atomic-updates, no-underscore-dangle
        instance._poller = setInterval(() => {
            instance.refresh()
        }, pollInterval)
        return instance
    }

    getStreams() {
        return Array.from(this.streams.values()).map((key) => getStreamFromKey(key))
    }

    addChangeListener(listener) {
        this.listeners.push(listener)
    }

    _setStreams(streamKeys) {
        const oldKeys = this.streams
        const newKeys = new Set(streamKeys)
        const added = new Set([...newKeys].filter((x) => !oldKeys.has(x)))
        const removed = new Set([...oldKeys].filter((x) => !newKeys.has(x)))
        this.streams = newKeys
        this.listeners.forEach((listener) => {
            added.forEach((key) => listener.onStreamAdded(getStreamFromKey(key)))
            removed.forEach((key) => listener.onStreamRemoved(getStreamFromKey(key)))
        })
    }

    refresh() {
        return fetch(`${this.apiUrl}/storageNodes/${this.nodeId}/streams`)
            .then((res) => res.json())
            .then((json) => {
                const streamKeys = []
                json.forEach((stream) => {
                    for (let i = 0; i < stream.partitions; i++) {
                        streamKeys.push(getKeyFromStream(stream.id, i))
                    }
                })
                this._setStreams(streamKeys)
                return undefined
            })
    }

    cleanup() {
        if (this._poller !== undefined) {
            clearInterval(this._poller)
        }
    }
}
