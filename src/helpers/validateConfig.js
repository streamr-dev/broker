const MissingConfigError = require('../errors/MissingConfigError')

const validateConfig = (config) => {
    if (config.network === undefined) {
        throw new MissingConfigError('network')
    }
    if (config.network.id === undefined) {
        throw new MissingConfigError('network.id')
    }
    if (config.network.hostname === undefined) {
        throw new MissingConfigError('network.hostname')
    }
    if (config.network.port === undefined) {
        throw new MissingConfigError('network.port')
    }
    if (config.network.advertisedWsUrl === undefined) {
        throw new MissingConfigError('network.advertisedWsUrl')
    }
    if (config.network.tracker === undefined && config.network.trackers === undefined) {
        throw new MissingConfigError('network.tracker or network.trackers')
    }
    if (config.network.trackers && !Array.isArray(config.network.trackers)) {
        throw new MissingConfigError('network.trackers must be array')
    }
    if (config.network.isStorageNode === undefined) {
        throw new MissingConfigError('network.isStorageNode')
    }
    if (config.cassandra === undefined) {
        throw new MissingConfigError('cassandra')
    }
    if (config.cassandra && config.cassandra.hosts === undefined) {
        throw new MissingConfigError('cassandra.hosts')
    }
    if (config.cassandra && config.cassandra.username === undefined) {
        throw new MissingConfigError('cassandra.username')
    }
    if (config.cassandra && config.cassandra.password === undefined) {
        throw new MissingConfigError('cassandra.password')
    }
    if (config.cassandra && config.cassandra.keyspace === undefined) {
        throw new MissingConfigError('cassandra.keyspace')
    }
    if (config.streamrUrl === undefined) {
        throw new MissingConfigError('streamrUrl')
    }
    if (config.adapters === undefined) {
        throw new MissingConfigError('adapters')
    }
    if (config.reporting === undefined) {
        throw new MissingConfigError('reporting')
    }
    if (config.reporting && (config.reporting.streamId !== undefined || config.reporting.apiKey !== undefined)) {
        if (config.reporting.apiKey === undefined) {
            throw new MissingConfigError('reporting.apiKey')
        }
        if (config.reporting.streamId === undefined) {
            throw new MissingConfigError('reporting.streamId')
        }
    }
    if (config.reporting && config.reporting.reportingIntervalSeconds === undefined) {
        throw new MissingConfigError('reporting.reportingIntervalSeconds')
    }
    if (config.sentry === undefined) {
        throw new MissingConfigError('sentry')
    }
    if (config.trackerRegistry && config.trackerRegistry.config === undefined) {
        throw new MissingConfigError('trackerRegistry.config')
    }
    if (config.trackerRegistry && config.trackerRegistry.jsonRpcProvider === undefined) {
        throw new MissingConfigError('trackerRegistry.jsonRpcProvider')
    }
    if (config.trackerRegistry && config.trackerRegistry.address === undefined) {
        throw new MissingConfigError('trackerRegistry.address')
    }

    config.adapters.forEach(({ name }, index) => {
        if (name === undefined) {
            throw new MissingConfigError(`adapters[${index}].name`)
        }
    })
}

module.exports = validateConfig
