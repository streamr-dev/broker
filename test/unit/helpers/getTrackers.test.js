const getTrackers = require('../../../src/helpers/getTrackers')

const address = '0xBFCF120a8fD17670536f1B27D9737B775b2FD4CF'
const config = 'TrackerRegistryDev.json'
const jsonRpcProvider = 'http://localhost:8545'

describe('getTrackers', () => {
    test('get array of trackers', async () => {
        const trackers = await getTrackers(address, config, jsonRpcProvider)
        expect(trackers).toStrictEqual([
            'ws://10.200.10.1:30301',
            'ws://10.200.10.1:30302',
            'ws://10.200.10.1:30303'
        ])
    })

    test('throw exception if address is wrong', async (done) => {
        try {
            await getTrackers('address', config, jsonRpcProvider)
        } catch (e) {
            done()
        }
    })

    test('throw exception if config is wrong', async (done) => {
        try {
            await getTrackers(address, 'config', jsonRpcProvider)
        } catch (e) {
            done()
        }
    })

    test('throw exception if jsonRpcProvider is wrong', async (done) => {
        try {
            await getTrackers(address, config, 'jsonRpcProvider')
        } catch (e) {
            done()
        }
    })
})
