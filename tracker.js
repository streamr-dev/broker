#!/usr/bin/env node
const { spawn } = require('child_process')
const path = require('path')
const program = require('commander')

const { authenticateFromConfig } = require('./src/helpers/ethereumAuthenticate')

program
    .option('-g, --generateKey', 'Generate random wallet', false)
    .option('-p, --privateKey <privateKey>', 'Ethereum private key', null)
    .option('-n, --trackerName <trackerName>', 'Human readable name', null)
    .parse(process.argv)

if (!program.generateKey && !program.privateKey) {
    throw new Error('--generateKey (-g) or --privateKey (-p) parameter required')
}
const address = authenticateFromConfig({
    privateKey: program.privateKey,
    generateWallet: program.generateKey
})

const name = program.trackerName || address

spawn(path.resolve(__dirname, './node_modules/streamr-network/bin/tracker.js'), process.argv, {
    cwd: process.cwd(),
    detached: false,
    stdio: 'inherit'
})
