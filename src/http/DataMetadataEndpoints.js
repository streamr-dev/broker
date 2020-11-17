const express = require('express')

module.exports = () => {
    const router = express.Router()

    router.get(
      '/streams/:id/metadata/',
      (req, res) => {
        res.status(200).send({
           totalBytes: 0,
           totalMessages: 0,
           firstMessage: 0,
           lastMessage: 0
        })
      }
    )

    return router
}
