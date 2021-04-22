const { wait } = require('streamr-test-utils')

const { SubscriptionManager } = require('../../src/SubscriptionManager')

describe('SubscriptionManager', () => {
    let networkNode
    let manager

    beforeEach(() => {
        networkNode = {
            subscribe: jest.fn(),
            unsubscribe: jest.fn()
        }
        manager = new SubscriptionManager(networkNode, 50)
    })

    it('calls networkNode#subscribe on each subscribe', () => {
        manager.subscribe('stream-1', 0)
        manager.subscribe('stream-1', 0)
        manager.subscribe('stream-1', 5)
        manager.subscribe('stream-1', 0)
        manager.subscribe('stream-2', 0)

        expect(networkNode.subscribe.mock.calls).toEqual([
            ['stream-1', 0],
            ['stream-1', 0],
            ['stream-1', 5],
            ['stream-1', 0],
            ['stream-2', 0],
        ])
    })

    it('does not call networkNode#unsubscribe if subscribers still left', () => {
        manager.subscribe('stream-1', 0)
        manager.subscribe('stream-1', 0)
        manager.subscribe('stream-1', 0)

        manager.unsubscribe('stream-1', 0)
        manager.unsubscribe('stream-1', 0)

        expect(networkNode.unsubscribe).toHaveBeenCalledTimes(0)
    })

    it('calls networkNode#unsubscribe if no subscribers left', () => {
        manager.subscribe('stream-1', 0)
        manager.subscribe('stream-1', 0)
        manager.subscribe('stream-1', 0)

        manager.unsubscribe('stream-1', 0)
        manager.unsubscribe('stream-1', 0)
        manager.unsubscribe('stream-1', 0)

        expect(networkNode.unsubscribe).toHaveBeenCalledTimes(1)
    })

    it('does not call networkNode#unsubscribe if calling unsubscribe on stream that isn\'t subscribed to', () => {
        manager.unsubscribe('stream-1', 0)

        expect(networkNode.unsubscribe).toHaveBeenCalledTimes(0)
    })

    it('does not call networkNode#unsubscribe if calling unsubscribe if recordPublish active', () => {
        manager.subscribe('stream-1', 0)
        manager.recordPublish('stream-1', 0)

        manager.unsubscribe('stream-1', 0)

        expect(networkNode.unsubscribe).toHaveBeenCalledTimes(0)
    })

    it('recordPublish subscription eventually wears off', async () => {
        manager.subscribe('stream-1', 0)
        manager.recordPublish('stream-1', 0)

        manager.unsubscribe('stream-1', 0)

        await wait(51)

        expect(networkNode.unsubscribe).toHaveBeenCalledTimes(1)
    })

    it('does not call networkNode#unsubscribe if calling unsubscribe multiple times when recordPublish active', () => {
        manager.subscribe('stream-1', 0)
        manager.recordPublish('stream-1', 0)

        manager.unsubscribe('stream-1', 0)
        manager.unsubscribe('stream-1', 0)
        manager.unsubscribe('stream-1', 0)
        manager.unsubscribe('stream-1', 0)

        expect(networkNode.unsubscribe).toHaveBeenCalledTimes(0)
    })

    it('clearing timeout', async () => {
        manager.recordPublish('stream-1', 0)
        manager.recordPublish('stream-2', 0)
        manager.recordPublish('stream-3', 0)

        manager.clear()

        await wait(51)

        expect(networkNode.unsubscribe).toHaveBeenCalledTimes(0)
    })
})
