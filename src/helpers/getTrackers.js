const { Contract, providers: { JsonRpcProvider } } = require('ethers')

const getTrackers = async (address, config, jsonRpcProvider) => {
    // eslint-disable-next-line import/no-dynamic-require,global-require
    const trackerRegistryConfig = require(`../../configs/${config}`)

    const provider = new JsonRpcProvider(jsonRpcProvider)
    // check that provider is connected and has some valid blockNumber
    await provider.getBlockNumber()

    const contract = new Contract(address, trackerRegistryConfig.abi, provider)
    // check that contract is connected
    await contract.addressPromise

    const trackers = []
    const result = await contract.getNodes()
    result.forEach((node) => {
        trackers.push(node.url)
    })

    return trackers
}

module.exports = getTrackers
