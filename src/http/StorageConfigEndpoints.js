const express = require('express')

const createHandler = (storageConfig) => {
    return (req, res) => {
        const { id, partition } = req.params
        const body = {
            persistent: storageConfig.hasStream({
                id,
                partition
            })
        }
        res.status(200).send(body)
    }
}

module.exports = (storageConfig) => {
    const router = express.Router()
    const handler = (storageConfig !== null) ? createHandler(storageConfig) : (_, res) => res.status(501).send('Not a storage node.')
    router.get('/streams/:id/storage/partitions/:partition', handler)
    return router
}
