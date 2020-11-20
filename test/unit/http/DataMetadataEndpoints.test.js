const sinon = require('sinon')
const express = require('express')
const request = require('supertest')
//const { StreamMessage, MessageID, MessageRef } = require('streamr-network').Protocol.MessageLayer

const dataMetadataEndpoint = require('../../../src/http/DataMetadataEndpoints')



describe('DataMetadataEndpoints', () => {

    let app

    function testGetRequest(url, key = 'authKey') {
        return request(app)
            .get(url)
            .set('Accept', 'application/json')
            .set('Authorization', `Token ${key}`)
    }

    it ('should testGetRequest', async () => {
      app = express()
      app.use('/api/v1', dataMetadataEndpoint())

      await testGetRequest('/api/v1/streams/0/metadata/partitions/0')
      .expect('Content-Type', /json/)
      .expect(200, {
        'totalBytes':0,
        'totalMessages':0,
        'firstMessage':0,
        'lastMessage':0
      })
    })
})
