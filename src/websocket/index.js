const MissingConfigError = require('../errors/MissingConfigError')
const adapterRegistry = require('../adapterRegistry')

const WebsocketServer = require('./WebsocketServer')

adapterRegistry.register('ws', ({ port }, {
    networkNode, publisher, streamFetcher, volumeLogger, subscriptionManager
}) => {
    if (port === undefined) {
        throw new MissingConfigError('port')
    }

    const websocketServer = new WebsocketServer(
        port,
        '/api/v1/ws',
        networkNode,
        streamFetcher,
        publisher,
        volumeLogger,
        subscriptionManager
    )
    return () => websocketServer.close()
})
