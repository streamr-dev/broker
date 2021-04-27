import express, { Request, Response } from 'express'
import { Storage } from '../storage/Storage'

const parseIntIfExists = (x: string|undefined) => {
    return x === undefined ? undefined : parseInt(x)
}

export const router = (cassandraStorage: Storage) => {
    const router = express.Router()
    const handler = async (req: Request, res: Response) => {
        const streamId = req.params.id
        const partition = parseIntIfExists(req.params.partition) || 0

        const out = {
            totalBytes: await cassandraStorage.getTotalBytesInStream(streamId, partition),
            totalMessages: await cassandraStorage.getNumberOfMessagesInStream(streamId, partition),
            firstMessage: await cassandraStorage.getFirstMessageTimestampInStream(streamId, partition),
            lastMessage: await cassandraStorage.getLastMessageTimestampInStream(streamId, partition)
        }

        res.status(200).send(out)
    }

    router.get(
        '/streams/:id/metadata/partitions/:partition',
        handler
    )

    return router
}
