const events = require('events')

const sinon = require('sinon')
const { StreamMessage, MessageID } = require('streamr-client-protocol').MessageLayer

const Publisher = require('../../src/Publisher')
const { MessageNotSignedError, MessageNotEncryptedError } = require('../../src/errors/MessageNotSignedError')

describe('Publisher', () => {
    const stream = {
        id: 'streamId',
        partitions: 10
    }

    const msg = {
        hello: 'world'
    }

    const streamMessage = new StreamMessage(
        new MessageID(stream.id, 0, 135135135, 0, 'publisherId', 'msgChainId'),
        null,
        JSON.stringify(msg)
    )

    let networkNode
    let validator

    const getPublisher = () => new Publisher(networkNode, validator)

    beforeEach(() => {
        networkNode = new events.EventEmitter()
        networkNode.publish = sinon.stub().resolves()
        validator = {
            validate: sinon.stub().resolves()
        }
    })

    describe('validateAndPublish', () => {
        it('calls the validator', async () => {
            await getPublisher().validateAndPublish(streamMessage)
            expect(validator.validate.calledWith(streamMessage)).toBe(true)
        })

        it('throws on invalid messages', async () => {
            validator = {
                validate: sinon.stub().rejects()
            }
            await expect(getPublisher().validateAndPublish(streamMessage)).rejects.toThrow()
        })

        it('should call NetworkNode.publish with correct values', async () => {
            await getPublisher().validateAndPublish(streamMessage)
            expect(networkNode.publish.calledWith(streamMessage)).toBe(true)
        })
    })
})
