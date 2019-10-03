# Broker

Main executable for running a broker node in Streamr Network.

The broker node extends the minimal network node provided by the
[network library]([network library](https://github.com/streamr-dev/network)) with
- client-facing support for foreign protocols (e.g. HTTP, MQTT) via adapters
- support for long-term persistence of data via Apache Cassandra.

## Developing
Project uses npm for package management.

- Start off by installing required dependencies with `npm install`
- To run tests `npm test`

## Running
First install the package
```
npm install -g @streamr/broker
```

Then run the command broker with the desired configuration file
```
broker <configFile>
```

See folder "configs" for example configurations, e.g., to run a simple local broker
```
broker configs/development-1.env.json
```


## Publishing

Publishing to NPM is automated via Travis CI. Follow the steps below to publish.

1. Update version with either `npm version patch`, `npm version minor`, or `npm version major`. Use semantic versioning
https://semver.org/. Files package.json and package-lock.json will be automatically updated, and an appropriate git commit and tag created. 
2. `git push --follow-tags`
3. Wait for Travis CI to run tests
4. If tests passed, Travis CI will publish the new version to NPM

## API Specification

For production version refer to https://www.streamr.com/help/api#datainput and https://www.streamr.com/help/api#dataoutput.

Otherwise see [APIDOC.md](APIDOC.md).

## Protocol Specification

Messaging protocol is described in [streamr-specs PROTOCOL.md](https://github.com/streamr-dev/streamr-specs/blob/master/PROTOCOL.md).

## MQTT publishing

- For authentication put API_KEY in password connection field
- MQTT native clients are able to send plain text, but their payload will be transformed to JSON
`{"mqttPayload":"ORIGINAL_PLAINTEXT_PAYLOAD}`

Error handling:
- If API_KEY is not correct, client will receive "Connection refused, bad user name or password" (returnCode: 4)
- If stream is not found, client will receive "Connection refused, not authorized" (returnCode: 5)


## License

This software is open source, and dual licensed under [AGPLv3](https://www.gnu.org/licenses/agpl.html) and an enterprise-friendly commercial license.
