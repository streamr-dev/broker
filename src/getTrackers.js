const { Contract, providers: { JsonRpcProvider } } = require('ethers')

async function getTrackers(address, config, jsonRpcProvider) {
    // eslint-disable-next-line import/no-dynamic-require,global-require
    const trackerRegistryConfig = require(`../configs/${config}`)
    const provider = new JsonRpcProvider(jsonRpcProvider)
    const contract = new Contract(address, trackerRegistryConfig.abi, provider)

    const trackers = []
    const result = await contract.getNodes()
    result.forEach((node) => {
        trackers.push(node.url)
    })

    return trackers
}

module.exports = getTrackers
