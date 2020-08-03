const ethers = require('ethers')

async function authenticateFromConfig(ethereumConfig, log = console.info) {
    let wallet
    let provider

    if (ethereumConfig.url) {
        provider = new ethers.providers.JsonRpcProvider(ethereumConfig.url)
        // eslint-disable-next-line no-underscore-dangle
        await provider._networkPromise
    }
    if (ethereumConfig.mnemonic) {
        log('Ethereum authentication with mnemonic')
        try {
            wallet = await ethers.Wallet.fromMnemonic(ethereumConfig.mnemonic, provider)
        } catch (e) {
            throw new Error(e)
        }
    } else if (ethereumConfig.privateKey) {
        log('Ethereum Authentication with private key')
        try {
            wallet = await new ethers.Wallet(ethereumConfig.privateKey, provider)
        } catch (e) {
            throw new Error(e)
        }
    } else if (ethereumConfig.generateWallet) {
        log('Ethereum authentication with new randomly generated wallet')
        try {
            wallet = await ethers.Wallet.createRandom()
        } catch (e) {
            throw new Error(e)
        }
    } else {
        log('Ethereum authentication disabled')
    }
    return wallet || {}
}

module.exports = {
    authenticateFromConfig
}
