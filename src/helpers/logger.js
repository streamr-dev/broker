const pino = require('pino')

const getLogger = (name) => pino({
    name,
    enabled: !process.env.NOLOG,
    level: process.env.LOG_LEVEL || 'info',
    prettyPrint: {
        colorize: true,
        translateTime: true
    }
})

module.exports = getLogger
