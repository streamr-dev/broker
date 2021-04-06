const assert = require('assert')

const StreamStateManager = require('../../../src/StreamStateManager')

jest.useFakeTimers()

describe('StreamStateManager', () => {
    let streams

    beforeEach(() => {
        streams = new StreamStateManager()
    })

    afterEach(() => {
        streams.close()
    })

    describe('create', () => {
        it('returns an object with the correct id, partition and state', () => {
            const stream = streams.create('streamid', 3)
            assert.equal(stream.id, 'streamid')
            assert.equal(stream.partition, 3)
            assert.equal(stream.state, 'init')
        })

        it('attempting to create same stream twice throws Error', () => {
            streams.create('streamid', 3)
            expect(() => streams.create('streamid', 3)).toThrowError()
        })
    })

    describe('get', () => {
        let stream
        beforeEach(() => {
            stream = streams.create('streamid', 4)
        })

        it('returns the requested stream', () => {
            assert.equal(streams.get('streamid', 4), stream)
        })

        it('returns undefined if the stream does not exist', () => {
            assert.equal(streams.get('streamid', 1), undefined)
        })
    })

    describe('getOrCreate', () => {
        it('creates stream if one does not yet exist', () => {
            const stream = streams.getOrCreate('streamid', 4)
            assert.deepStrictEqual(stream.id, 'streamid')
            assert.deepStrictEqual(stream.partition, 4)
        })

        it('returns existing stream if one exists', () => {
            const firstStream = streams.getOrCreate('streamid', 4)
            const secondStream = streams.getOrCreate('streamid', 4)
            expect(firstStream).toBe(secondStream)
        })
    })

    describe('deleteStreamObject', () => {
        beforeEach(() => {
            streams.create('streamid', 0)
        })

        it('must delete the requested stream', () => {
            streams.delete('streamid', 0)
            assert.equal(streams.get('streamid', 0), undefined)
        })
    })

    describe('timeout behavior', () => {
        let stream

        beforeEach(() => {
            stream = streams.create('streamid', 0)
        })

        it('stream object is deleted after 60 seconds if state remains unchanged', () => {
            jest.advanceTimersByTime(60 * 1000)
            expect(streams.get('streamid', 0)).toBeUndefined()
        })

        it('stream object is deleted after 60 seconds if state is subscribing', () => {
            stream.setSubscribing()
            jest.advanceTimersByTime(60 * 1000)
            expect(streams.get('streamid', 0)).toBeUndefined()
        })

        it('stream object remains after 60 seconds if state is subscribed', () => {
            jest.advanceTimersByTime(59 * 1000)
            stream.setSubscribed()
            jest.advanceTimersByTime(1000)
            expect(streams.get('streamid', 0)).not.toBeUndefined()
        })
    })
})
