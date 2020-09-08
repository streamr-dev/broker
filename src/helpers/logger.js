const pino = require('pino')

const getLogger = (name) => pino({
    name
})

module.exports = getLogger
