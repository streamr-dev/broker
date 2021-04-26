const express = require('express')

const createHandler = (storageConfig) => {
    return (req, res) => {
        const { id, partition } = req.params
        const isValidPartition = !Number.isNaN(parseInt(partition))
        if (isValidPartition) {
            const found = storageConfig.hasStream({
                id,
                partition: Number(partition)
            })
            if (found) {
                res.status(200).send({})
            } else {
                res.status(404).end()
            }
        } else {
            res.status(400).send('Partition is not a number: ' + partition)
        }
    }
}

module.exports = (storageConfig) => {
    const router = express.Router()
    const handler = createHandler(storageConfig)
    router.get('/streams/:id/storage/partitions/:partition', handler)
    return router
}
