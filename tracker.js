#!/usr/bin/env node
const { spawn } = require('child_process')
const path = require('path')
const program = require('commander')

const { authenticateFromConfig } = require('./src/helpers/ethereumAuthenticate')

program
    .usage('<privateKey>')
    .parse(process.argv)

if (program.args.length < 2) {
    program.help()
}
const privateKey = program.args[0]
const trackerName = program.args[1]

const address = authenticateFromConfig({
    privateKey
})

process.argv.push(['--trackerName', trackerName])

spawn(path.resolve(__dirname, './node_modules/streamr-network/bin/tracker.js'), process.argv, {
    cwd: process.cwd(),
    detached: false,
    stdio: 'inherit'
})
