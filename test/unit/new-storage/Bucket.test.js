const Bucket = require('../../../src/new-storage/Bucket')

describe('Bucket', () => {
    it('should throw if constructor parameters are not correct', () => {
        expect(() => {
            const a = new Bucket()
        }).toThrow(new TypeError('id must be not empty string'))

        expect(() => {
            const a = new Bucket('id')
        }).toThrow(new TypeError('streamId must be not empty string'))

        expect(() => {
            const a = new Bucket('id', 'streamId')
        }).toThrow(new TypeError('partition must be >= 0'))

        expect(() => {
            const a = new Bucket('id', 'streamId', 0)
        }).toThrow(new TypeError('size must be => 0'))

        expect(() => {
            const a = new Bucket('id', 'streamId', 0, 0)
        }).toThrow(new TypeError('records must be => 0'))

        expect(() => {
            const a = new Bucket('id', 'streamId', 0, 0, 0)
        }).toThrow(new TypeError('dateCreate must be instance of Date'))

        expect(() => {
            const a = new Bucket('id', 'streamId', 0, 0, 0, new Date('2019-07-19'))
        }).toThrow(new TypeError('maxSize must be > 0'))

        expect(() => {
            const a = new Bucket('id', 'streamId', 0, 0, 0, new Date('2019-07-19'), 1)
        }).toThrow(new TypeError('maxRecords must be > 0'))

        expect(() => {
            const a = new Bucket('id', 'streamId', 0, 0, 0, new Date('2019-07-19'), 1, 1)
        }).toThrow(new TypeError('keepAliveSeconds must be > 0'))

        expect(() => {
            const a = new Bucket('id', 'streamId', 0, 0, 0, new Date('2019-07-19'), 1, 1, 1)
        }).not.toThrow()
    })

    it('incrementBucket and isFull', () => {
        const bucket = new Bucket('id', 'streamId', 0, 0, 0, new Date(), 3, 9, 1)

        expect(bucket.isFull()).toBeFalsy()
        bucket.incrementBucket(1, 3)
        expect(bucket.isFull()).toBeFalsy()

        bucket.incrementBucket(1, 3)
        expect(bucket.isFull()).toBeFalsy()

        bucket.incrementBucket(1, 3)
        expect(bucket.isFull()).toBeTruthy()

        expect(bucket.size).toEqual(3)
        expect(bucket.records).toEqual(9)
    })

    it('isAlive and getId', (done) => {
        const bucket = new Bucket('id', 'streamId', 0, 0, 0, new Date(), 3, 9, 1)

        expect(bucket.getId()).toEqual('id')
        expect(bucket.isAlive()).toBeTruthy()

        setTimeout(() => {
            expect(bucket.isAlive()).toBeFalsy()
            done()
        }, 1000 + 1)
    })

    it('isStored', () => {
        const bucket = new Bucket('id', 'streamId', 0, 0, 0, new Date(), 3, 9, 1)
        expect(bucket.isStored()).toBeFalsy()
        bucket.incrementBucket(1, 1)
        expect(bucket.isStored()).toBeFalsy()
    })
})

