const pino = require('pino')

const getLogger = (name) => pino({
    name,
    prettyPrint: {
        colorize: true,
        translateTime: true
    }
})

module.exports = getLogger
