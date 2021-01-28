#!/usr/bin/env node

// To run the test:
// 1. start tracker: ./tracker.js 0xc2efccb8685e980c7cb15bc5de5b72304d85883d844ff975bcf32200fa594bc6 tracker-test
// 2. start broker: ./broker.js ../configs/development-1.env.json
// 3. run this file: ./eventbus-test.js

const StreamrClient = require('streamr-client')
const ethers = require('ethers')
const fetch = require('node-fetch')

const storageNodePrivateKey = '0xaa7a3b3bb9b4a662e756e978ad8c6464412e7eef1b871f19e5120d4747bce966'
const coreApiPrivateKey = '0x0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF'
const userPrivateKey = ethers.Wallet.createRandom().privateKey
const storageNodeAccount = new ethers.Wallet(storageNodePrivateKey)
const eventBusStreamId = storageNodeAccount.address + '/eventBus'

const WS_URL = 'ws://localhost/api/v1/ws'
const API_URL = 'http://localhost/api/v1'

const createClient = (privateKey) => {
    return new StreamrClient({
        auth: {
            privateKey
        },
        url: WS_URL,
        restUrl: API_URL
    })
}

const main = async () => {
    // 1. User calls Core API to add a stream to storage node:
    const userClient = await createClient(userPrivateKey)
    const newStream = await userClient.createStream()
    console.log('User created stream: ' + newStream.id)
    await fetch(`${API_URL}/streams/${encodeURIComponent(newStream.id)}/storageNodes`, {
        body: JSON.stringify({
            address: storageNodeAccount.address
        }),
        headers: {
            // eslint-disable-next-line quote-props
            'Authorization': 'Bearer ' + await userClient.session.getSessionToken(),
            'Content-Type': 'application/json',
        },
        method: 'POST'
    })
    console.log('User added the stream to storage node: ' + storageNodeAccount.address)

    // 2. User listens to the eventBus stream and sees when that stream has been added
    userClient.subscribe(eventBusStreamId, (message) => {
        if (message.event === 'storageConfigRefreshCompleted') {
            console.log('Storage config refreshed, contains these streamParts: ' + JSON.stringify(message.streamParts))
        }
    })

    // 3. Core API receives the "add storage node" API requests and stores the a new row to the DB (as usually)
    // - and it also notifies the storage node by writing to the storage node's eventBus stream
    const message = {
        command: 'requestStorageConfigRefresh'
    }
    const coreApiClient = await createClient(coreApiPrivateKey)
    coreApiClient.publish(eventBusStreamId, message)
    console.log('Core API requested the storage node to refresh the config: ' + eventBusStreamId + ' ' + JSON.stringify(message))
}

main()
