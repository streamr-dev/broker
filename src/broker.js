const { startNetworkNode, startStorageNode, Protocol, MetricsContext } = require('streamr-network')
const pino = require('pino')
const StreamrClient = require('streamr-client')
const publicIp = require('public-ip')
const Sentry = require('@sentry/node')
const ethers = require('ethers')

const CURRENT_VERSION = require('../package.json').version

const logger = require('./helpers/logger')('streamr:broker')
const StreamFetcher = require('./StreamFetcher')
const { startCassandraStorage } = require('./storage/Storage')
const Publisher = require('./Publisher')
const VolumeLogger = require('./VolumeLogger')
const SubscriptionManager = require('./SubscriptionManager')
const MissingConfigError = require('./errors/MissingConfigError')
const adapterRegistry = require('./adapterRegistry')
const validateConfig = require('./helpers/validateConfig')

const { Utils } = Protocol

module.exports = async (config) => {
    validateConfig(config)

    logger.info(`Starting broker version ${CURRENT_VERSION}`)

    const networkNodeName = config.network.name
    const metricsContext = new MetricsContext(networkNodeName)
    const storages = []

    // Ethereum wallet retrieval
    const wallet = new ethers.Wallet(config.ethereumPrivateKey)
    if (!wallet) {
        throw new Error('Could not resolve Ethereum address from given config.ethereumPrivateKey')
    }
    const brokerAddress = wallet.address

    let cassandraStorage
    // Start cassandra storage
    if (config.cassandra) {
        logger.info(`Starting Cassandra with hosts ${config.cassandra.hosts} and keyspace ${config.cassandra.keyspace}`)
        cassandraStorage = await startCassandraStorage({
            contactPoints: [...config.cassandra.hosts],
            localDataCenter: config.cassandra.datacenter,
            keyspace: config.cassandra.keyspace,
            username: config.cassandra.username,
            password: config.cassandra.password,
            opts: {
                useTtl: !config.network.isStorageNode
            }
        })
        cassandraStorage.enableMetrics(metricsContext)
        storages.push(cassandraStorage)
    } else {
        logger.info('Cassandra disabled')
    }

    // Form tracker list
    let trackers
    if (config.network.trackers.registryAddress) {
        const registry = await Protocol.Utils.getTrackerRegistryFromContract({
            contractAddress: config.network.trackers.registryAddress,
            jsonRpcProvider: config.network.trackers.jsonRpcProvider
        })
        trackers = registry.getAllTrackers().map((record) => record.ws)
    } else {
        trackers = config.network.trackers
    }

    // Start network node
    const startFn = config.network.isStorageNode ? startStorageNode : startNetworkNode
    const advertisedWsUrl = config.network.advertisedWsUrl !== 'auto'
        ? config.network.advertisedWsUrl
        : await publicIp.v4().then((ip) => `ws://${ip}:${config.network.port}`)
    const networkNode = await startFn({
        host: config.network.hostname,
        port: config.network.port,
        id: brokerAddress,
        name: networkNodeName,
        trackers,
        storages,
        advertisedWsUrl,
        location: config.network.location,
        metricsContext
    })
    networkNode.start()

    // Set up sentry logging
    if (config.reporting.sentry) {
        logger.info('Starting Sentry with dns: %s', config.reporting.sentry)
        Sentry.init({
            dsn: config.reporting.sentry,
            integrations: [
                new Sentry.Integrations.Console({
                    levels: ['error']
                })
            ],
            environment: config.network.hostname,
            maxBreadcrumbs: 50,
            attachStacktrace: true,

        })

        Sentry.configureScope((scope) => {
            scope.setUser({
                id: networkNodeName
            })
        })
    }

    // Set up reporting to Streamr stream
    let client
    let streamId
    let apiKey
    if (config.reporting.streamr) {
        streamId = config.reporting.streamr.streamId
        apiKey = config.reporting.streamr.apiKey
        logger.info(`Starting StreamrClient reporting with apiKey: ${apiKey} and streamId: ${streamId}`)

        client = new StreamrClient({
            auth: {
                apiKey
            },
            autoConnect: false
        })
    } else {
        logger.info('StreamrClient reporting disabled')
    }

    let volumeLogger
    if (config.reporting.perNodeMetrics && config.reporting.perNodeMetrics.enabled) {
        // set up stream-specific reporting metrics
        client = new StreamrClient({
            auth: {
                privateKey: config.ethereumPrivateKey
            },
            url: config.reporting.perNodeMetrics.wsUrl,
            restUrl: config.reporting.perNodeMetrics.httpUrl
            // autoConnect: false
        })

        const metricsStream = await client.getOrCreateStream({
            name: brokerAddress,
            id: brokerAddress + '/streamr/node/metrics/sec'
        })

        await metricsStream.grantPermission('stream_get', null)
        await metricsStream.grantPermission('stream_subscribe', null)

        // Initialize common utilities
        volumeLogger = new VolumeLogger(
            config.reporting.intervalInSeconds,
            metricsContext,
            client,
            metricsStream.id
        )
    } else {
      volumeLogger = new VolumeLogger(
          config.reporting.intervalInSeconds,
          metricsContext,
          client,
          streamId
      )
    }

    /*
    // Initialize common utilities
    const volumeLogger = new VolumeLogger(
        config.reporting.intervalInSeconds,
        metricsContext,
        client,
        streamId
    ) */
    // Validator only needs public information, so use unauthenticated client for that
    const unauthenticatedClient = new StreamrClient({
        restUrl: config.streamrUrl + '/api/v1',
    })
    const streamMessageValidator = new Utils.CachingStreamMessageValidator({
        getStream: (sId) => unauthenticatedClient.getStreamValidationInfo(sId),
        isPublisher: (address, sId) => unauthenticatedClient.isStreamPublisher(sId, address),
        isSubscriber: (address, sId) => unauthenticatedClient.isStreamSubscriber(sId, address),
    })
    const streamFetcher = new StreamFetcher(config.streamrUrl)
    const publisher = new Publisher(networkNode, streamMessageValidator, metricsContext)
    const subscriptionManager = new SubscriptionManager(networkNode)

    // Start up adapters one-by-one, storing their close functions for further use
    const closeAdapterFns = config.adapters.map(({ name, ...adapterConfig }, index) => {
        try {
            return adapterRegistry.startAdapter(name, adapterConfig, {
                networkNode,
                publisher,
                streamFetcher,
                metricsContext,
                subscriptionManager,
                cassandraStorage
            })
        } catch (e) {
            if (e instanceof MissingConfigError) {
                throw new MissingConfigError(`adapters[${index}].${e.config}`)
            }
            logger.error(`Error thrown while starting adapter ${name}: ${e}`)
            logger.error(e.stack)
            return () => {}
        }
    })

    logger.info(`Network node '${networkNodeName}' running on ${config.network.hostname}:${config.network.port}`)
    logger.info(`Ethereum address ${brokerAddress}`)
    logger.info(`Configured with trackers: ${trackers.join(', ')}`)
    logger.info(`Configured with Streamr: ${config.streamrUrl}`)
    logger.info(`Adapters: ${JSON.stringify(config.adapters.map((a) => a.name))}`)
    if (config.cassandra) {
        logger.info(`Configured with Cassandra: hosts=${config.cassandra.hosts} and keyspace=${config.cassandra.keyspace}`)
    }
    if (advertisedWsUrl) {
        logger.info(`Advertising to tracker WS url: ${advertisedWsUrl}`)
    }

    return {
        getStreams: () => networkNode.getStreams(),
        close: () => Promise.all([
            networkNode.stop(),
            ...closeAdapterFns.map((close) => close()),
            ...storages.map((storage) => storage.close()),
            volumeLogger.close()
        ])
    }
}

process.on('uncaughtException', pino.final(logger, (err, finalLogger) => {
    finalLogger.error(err, 'uncaughtException')
    process.exit(1)
}))

process.on('unhandledRejection', pino.final(logger, (err, finalLogger) => {
    finalLogger.error(err, 'unhandledRejection')
    process.exit(1)
}))
