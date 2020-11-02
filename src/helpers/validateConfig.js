const Ajv = require('ajv')

const BROKER_CONFIG_SCHEMA = require('./config.schema.json')

module.exports = function validateConfig(config) {
    const ajv = new Ajv()
    if (!ajv.validate(BROKER_CONFIG_SCHEMA, config)) {
        throw new Error(ajv.errorsText())
    }
}
