const MissingConfigError = require('../errors/MissingConfigError')
const adapterRegistry = require('../adapterRegistry')

adapterRegistry.register('mqtt', ({ port }, { networkNode, publisher, streamFetcher, volumeLogger }) => {
    if (port === undefined) {
        throw new MissingConfigError('port')
    }

    // Start MQTT server here
    // ...
    // ...
    setImmediate(() => {
        console.info(`MQTT server listening on ${port}`)
    })

    return () => {
        // close/shutdown MQTT server here
        // ...
        // ....
        console.info(`MQTT server closing on ${port}`)
    }
})
