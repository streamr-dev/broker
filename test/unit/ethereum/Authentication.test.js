const ethereumAuthenticate = require('../../../src/helpers/ethereumAuthenticate')
const { wait, waitForCondition } = require('streamr-test-utils')

// Dev env 1 privateKey and address
const privateKey = '0xaa7a3b3bb9b4a662e756e978ad8c6464412e7eef1b871f19e5120d4747bce966'
const address = '0xde1112f631486CfC759A50196853011528bC5FA0'

describe('Ethereum authentication', () => {
    it('authenticates with a private key', async (done) => {
        const config = {
            privateKey
        }
        const wallet = await ethereumAuthenticate.authenticateFromConfig(config)
        await waitForCondition(() => wallet)
        if (wallet.address === address) {
            done()
        }
    })
    it('authenticates with a randomly generated wallet', async (done) => {
        const config = {
            generateWallet: true
        }
        const wallet = await ethereumAuthenticate.authenticateFromConfig(config)
        await waitForCondition(() => wallet)
        if (wallet !== {}) {
            done()
        }
    })
    it('skips authentication if necessary', async (done) => {
        const config = {
            generateWallet: false
        }
        const wallet = await ethereumAuthenticate.authenticateFromConfig(config)
        await waitForCondition(() => wallet)
        if (!Object.keys(wallet).length) {
            done()
        }
    })
})