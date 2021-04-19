const express = require('express')
const request = require('supertest')
const intoStream = require('into-stream')
const { Protocol, MetricsContext } = require('streamr-network')

const { ControlLayer, MessageLayer } = Protocol
const { StreamMessage, MessageID } = MessageLayer

const restEndpointRouter = require('../../../src/http/DataQueryEndpoints')
const HttpError = require('../../../src/errors/HttpError')

describe('DataQueryEndpoints', () => {
    let app
    let networkNode
    let streamFetcher

    function testGetRequest(url, key = 'authKey') {
        return request(app)
            .get(url)
            .set('Accept', 'application/json')
            .set('Authorization', `Token ${key}`)
    }

    function createStreamMessage(content) {
        return new StreamMessage({
            messageId: new MessageID('streamId', 0, new Date(2017, 3, 1, 12, 0, 0).getTime(), 0, 'publisherId', '1'),
            content,
        })
    }

    function createUnicastMessage(streamMessage) {
        return new ControlLayer.UnicastMessage({
            requestId: 'requestId',
            streamMessage,
        })
    }

    beforeEach(() => {
        app = express()
        networkNode = {}
        streamFetcher = {
            async authenticate(_streamId, authKey) {
                if (authKey === 'authKey') {
                    return {}
                } else {
                    throw new HttpError(403, 'GET', '')
                }
            },
        }
        app.use('/api/v1', restEndpointRouter(networkNode, streamFetcher, new MetricsContext(null)))
    })

    describe('Getting last events', () => {
        let streamMessages

        beforeEach(() => {
            streamMessages = [
                createStreamMessage({
                    hello: 1,
                }),
                createStreamMessage({
                    world: 2,
                }),
            ]
            networkNode.requestResendLast = jest.fn(() => intoStream.object(
                streamMessages.map((m) => createUnicastMessage(m))
            ))
        })

        describe('user errors', () => {
            it('responds 400 and error message if param "partition" not a number', async () => {
                return testGetRequest('/api/v1/streams/streamId/data/partitions/zero/last')
                    .expect('Content-Type', /json/)
                    .expect(400, {
                        error: 'Path parameter "partition" not a number: zero',
                    })
            })

            it('responds 403 and error message if not authorized', async () => {
                return testGetRequest('/api/v1/streams/streamId/data/partitions/0/last', 'wrongKey')
                    .expect('Content-Type', /json/)
                    .expect(403, {
                        error: 'Authentication failed.',
                    })
            })

            it('responds 400 and error message if optional param "count" not a number', async () => {
                return testGetRequest('/api/v1/streams/streamId/data/partitions/0/last?count=sixsixsix')
                    .expect('Content-Type', /json/)
                    .expect(400, {
                        error: 'Query parameter "count" not a number: sixsixsix',
                    })
            })
        })

        describe('GET /api/v1/streams/streamId/data/partitions/0/last', () => {
            it('responds 200 and Content-Type JSON', async () => {
                return testGetRequest('/api/v1/streams/streamId/data/partitions/0/last')
                    .expect('Content-Type', /json/)
                    .expect(200)
            })

            it('responds with object representation of messages by default', async () => {
                return testGetRequest('/api/v1/streams/streamId/data/partitions/0/last')
                    .expect(streamMessages.map((m) => m.toObject()))
            })

            it('responds with latest version protocol serialization of messages given format=protocol', async () => {
                return testGetRequest('/api/v1/streams/streamId/data/partitions/0/last?format=protocol')
                    .expect(streamMessages.map((msg) => msg.serialize(StreamMessage.LATEST_VERSION)))
            })

            it('responds with specific version protocol serialization of messages given format=protocol&version=30', async () => {
                return testGetRequest('/api/v1/streams/streamId/data/partitions/0/last?format=protocol&version=30')
                    .expect(streamMessages.map((msg) => msg.serialize(30)),)
            })

            it('invokes networkNode#requestResendLast once with correct arguments', async () => {
                await testGetRequest('/api/v1/streams/streamId/data/partitions/0/last')
                expect(networkNode.requestResendLast).toHaveBeenCalledTimes(1)
                expect(networkNode.requestResendLast.mock.calls[0]).toEqual(['streamId', 0, expect.stringMatching(/\w+/), 1])
            })

            it('responds 500 and error message if networkNode signals error', async () => {
                networkNode.requestResendLast = () => intoStream.object(Promise.reject(new Error('expected error')))

                return testGetRequest('/api/v1/streams/streamId/data/partitions/0/last')
                    .expect('Content-Type', /json/)
                    .expect(500, {
                        error: 'Failed to fetch data!',
                    })
            })
        })

        describe('?count=666', () => {
            it('passes count to networkNode#requestResendLast', async () => {
                await testGetRequest('/api/v1/streams/streamId/data/partitions/0/last?count=666')

                expect(networkNode.requestResendLast).toHaveBeenCalledTimes(1)
                expect(networkNode.requestResendLast).toHaveBeenCalledWith(
                    'streamId',
                    0,
                    expect.stringMatching(/\w+/),
                    666,
                )
            })
        })
    })

    describe('From queries', () => {
        let streamMessages

        beforeEach(() => {
            streamMessages = [
                createStreamMessage({
                    a: 'a',
                }),
                createStreamMessage({
                    z: 'z',
                }),
            ]
            networkNode.requestResendFrom = jest.fn(() => intoStream.object(
                streamMessages.map((m) => createUnicastMessage(m))
            ))
            networkNode.requestResendLast = jest.fn(() => intoStream.object(
                streamMessages.map((m) => createUnicastMessage(m))
            ))
        })

        describe('?fromTimestamp=1496408255672', () => {
            it('responds 200 and Content-Type JSON', async () => {
                return testGetRequest('/api/v1/streams/streamId/data/partitions/0/from?fromTimestamp=1496408255672')
                    .expect('Content-Type', /json/)
                    .expect(200)
            })

            it('responds with data points as body', async () => {
                return testGetRequest('/api/v1/streams/streamId/data/partitions/0/from?fromTimestamp=1496408255672')
                    .expect(streamMessages.map((msg) => msg.toObject()))
            })

            it('invokes networkNode#requestResendFrom once with correct arguments', async () => {
                await testGetRequest('/api/v1/streams/streamId/data/partitions/0/from?fromTimestamp=1496408255672')

                expect(networkNode.requestResendFrom).toHaveBeenCalledTimes(1)
                expect(networkNode.requestResendFrom).toHaveBeenCalledWith(
                    'streamId',
                    0,
                    expect.stringMatching(/\w+/),
                    1496408255672,
                    undefined,
                    null,
                    null,
                )
            })

            it('responds 500 and error message if networkNode signals error', async () => {
                networkNode.requestResendFrom = jest.fn(() => intoStream.object(Promise.reject(new Error('expected error'))))

                return testGetRequest('/api/v1/streams/streamId/data/partitions/0/from?fromTimestamp=1496408255672')
                    .expect('Content-Type', /json/)
                    .expect(500, {
                        error: 'Failed to fetch data!',
                    })
            })
        })

        describe('?fromTimestamp=1496408255672&fromSequenceNumber=1&publisherId=publisherId', () => {
            const query = 'fromTimestamp=1496408255672&fromSequenceNumber=1&publisherId=publisherId'

            it('responds 200 and Content-Type JSON', async () => {
                return testGetRequest(`/api/v1/streams/streamId/data/partitions/0/from?${query}`)
                    .expect('Content-Type', /json/)
                    .expect(200)
            })

            it('responds with data points as body', async () => {
                return testGetRequest(`/api/v1/streams/streamId/data/partitions/0/from?${query}`)
                    .expect(streamMessages.map((msg) => msg.toObject()))
            })

            it('invokes networkNode#requestResendFrom once with correct arguments', async () => {
                await testGetRequest(`/api/v1/streams/streamId/data/partitions/0/from?${query}`)

                expect(networkNode.requestResendFrom).toHaveBeenCalledTimes(1)
                expect(networkNode.requestResendFrom).toHaveBeenCalledWith(
                    'streamId',
                    0,
                    expect.stringMatching(/\w+/),
                    1496408255672,
                    1,
                    'publisherId',
                    null,
                )
            })

            it('responds 500 and error message if networkNode signals error', async () => {
                networkNode.requestResendFrom = () => intoStream.object(Promise.reject(new Error('expected error')))

                return testGetRequest(`/api/v1/streams/streamId/data/partitions/0/from?${query}`)
                    .expect('Content-Type', /json/)
                    .expect(500, {
                        error: 'Failed to fetch data!',
                    })
            })
        })
    })

    describe('Range queries', () => {
        describe('user errors', () => {
            it('responds 400 and error message if param "partition" not a number', async () => {
                return testGetRequest('/api/v1/streams/streamId/data/partitions/zero/range')
                    .expect('Content-Type', /json/)
                    .expect(400, {
                        error: 'Path parameter "partition" not a number: zero',
                    })
            })
            it('responds 403 and error message if not authorized', async () => {
                return testGetRequest('/api/v1/streams/streamId/data/partitions/0/range', 'wrongKey')
                    .expect('Content-Type', /json/)
                    .expect(403, {
                        error: 'Authentication failed.',
                    })
            })
            it('responds 400 and error message if param "fromTimestamp" not given', async () => {
                return testGetRequest('/api/v1/streams/streamId/data/partitions/0/range')
                    .expect('Content-Type', /json/)
                    .expect(400, {
                        error: 'Query parameter "fromTimestamp" required.',
                    })
            })
            it('responds 400 and error message if param "fromTimestamp" not a number', async () => {
                return testGetRequest('/api/v1/streams/streamId/data/partitions/0/range?fromTimestamp=endOfTime')
                    .expect('Content-Type', /json/)
                    .expect(400, {
                        error: 'Query parameter "fromTimestamp" not a number: endOfTime',
                    })
            })
            it('responds 400 and error message if param "toTimestamp" not given', async () => {
                return testGetRequest('/api/v1/streams/streamId/data/partitions/0/range?fromTimestamp=1')
                    .expect('Content-Type', /json/)
                    .expect(400, {
                        error: 'Query parameter "toTimestamp" required as well. '
                        + 'To request all messages since a timestamp,'
                            + 'use the endpoint /streams/:id/data/partitions/:partition/from',
                    })
            })
            it('responds 400 and error message if optional param "toTimestamp" not a number', async () => {
                return testGetRequest('/api/v1/streams/streamId/data/partitions/0/range?fromTimestamp=1&toTimestamp=endOfLife')
                    .expect('Content-Type', /json/)
                    .expect(400, {
                        error: 'Query parameter "toTimestamp" not a number: endOfLife',
                    })
            })
        })

        describe('?fromTimestamp=1496408255672&toTimestamp=1496415670909', () => {
            let streamMessages
            beforeEach(() => {
                streamMessages = [
                    createStreamMessage([6, 6, 6]),
                    createStreamMessage({
                        '6': '6',
                    }),
                ]
                networkNode.requestResendRange = jest.fn(() => intoStream.object(
                    streamMessages.map((m) => createUnicastMessage(m))
                ))
            })

            it('responds 200 and Content-Type JSON', async () => {
                // eslint-disable-next-line max-len
                return testGetRequest('/api/v1/streams/streamId/data/partitions/0/range?fromTimestamp=1496408255672&toTimestamp=1496415670909')
                    .expect('Content-Type', /json/)
                    .expect(200)
            })

            it('responds with data points as body', async () => {
                // eslint-disable-next-line max-len
                return testGetRequest('/api/v1/streams/streamId/data/partitions/0/range?fromTimestamp=1496408255672&toTimestamp=1496415670909')
                    .expect(streamMessages.map((msg) => msg.toObject()))
            })

            it('invokes networkNode#requestResendRange once with correct arguments', async () => {
                // eslint-disable-next-line max-len
                await testGetRequest('/api/v1/streams/streamId/data/partitions/0/range?fromTimestamp=1496408255672&toTimestamp=1496415670909')

                expect(networkNode.requestResendRange).toHaveBeenCalledTimes(1)
                expect(networkNode.requestResendRange).toHaveBeenCalledWith(
                    'streamId',
                    0,
                    expect.stringMatching(/\w+/),
                    1496408255672,
                    undefined,
                    1496415670909,
                    undefined,
                    null,
                    null,
                )
            })

            it('responds 500 and error message if networkNode signals error', async () => {
                networkNode.requestResendRange = jest.fn(() => intoStream.object(Promise.reject(new Error('expected error'))))

                // eslint-disable-next-line max-len
                return testGetRequest('/api/v1/streams/streamId/data/partitions/0/range?fromTimestamp=1496408255672&toTimestamp=1496415670909')
                    .expect('Content-Type', /json/)
                    .expect(500, {
                        error: 'Failed to fetch data!',
                    })
            })
        })

        // eslint-disable-next-line max-len
        describe('?fromTimestamp=1496408255672&toTimestamp=1496415670909&fromSequenceNumber=1&toSequenceNumber=2&publisherId=publisherId', () => {
            // eslint-disable-next-line max-len
            const query = 'fromTimestamp=1496408255672&toTimestamp=1496415670909&fromSequenceNumber=1&toSequenceNumber=2&publisherId=publisherId'

            let streamMessages
            beforeEach(() => {
                streamMessages = [
                    createStreamMessage([6, 6, 6]),
                    createStreamMessage({
                        '6': '6',
                    }),
                ]
                networkNode.requestResendRange = jest.fn(() => intoStream.object(
                    streamMessages.map((m) => createUnicastMessage(m))
                ))
            })

            it('responds 200 and Content-Type JSON', async () => {
                return testGetRequest(`/api/v1/streams/streamId/data/partitions/0/range?${query}`)
                    .expect('Content-Type', /json/)
                    .expect(200)
            })

            it('responds with data points as body', async () => {
                return testGetRequest(`/api/v1/streams/streamId/data/partitions/0/range?${query}`)
                    .expect(streamMessages.map((msg) => msg.toObject()))
            })

            it('invokes networkNode#requestResendRange once with correct arguments', async () => {
                await testGetRequest(`/api/v1/streams/streamId/data/partitions/0/range?${query}`)
                expect(networkNode.requestResendRange).toHaveBeenCalledTimes(1)
                expect(networkNode.requestResendRange).toHaveBeenCalledWith(
                    'streamId',
                    0,
                    expect.stringMatching(/\w+/),
                    1496408255672,
                    1,
                    1496415670909,
                    2,
                    'publisherId',
                    null,
                )
            })

            it('responds 500 and error message if networkNode signals error', async () => {
                networkNode.requestResendRange = jest.fn(() => intoStream.object(Promise.reject(new Error('expected error'))))

                return testGetRequest(`/api/v1/streams/streamId/data/partitions/0/range?${query}`)
                    .expect('Content-Type', /json/)
                    .expect(500, {
                        error: 'Failed to fetch data!',
                    })
            })
        })
    })
})
