const isTimestampTooFarInTheFuture = (timestamp, thresholdSeconds) => {
    return timestamp > Date.now() + (thresholdSeconds * 1000)
}

module.exports = {
    isTimestampTooFarInTheFuture
}
