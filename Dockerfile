# Use official node runtime as base image
FROM node:8.11.4-alpine

# Set the working directory to /app
WORKDIR /app

# Copy app code
COPY . /app

# Install package.json dependencies (yes, clean up must be part of same RUN command because of layering)
RUN apk add --update python build-base && npm install && apk del python build-base && rm -rf /var/cache/apk/*

# Make ports available to the world outside this container
EXPOSE 30315
# WebSocket
EXPOSE 8890
# HTTP
EXPOSE 8891
# MQTT
EXPOSE 9000

ENV CASSANDRA_HOST cassandra
ENV STREAMR_URL http://127.0.0.1:8081/streamr-core
ENV TRACKER_URL ws://tracker:30300

CMD node app.js configs/development-1.env.json ${CASSANDRA_HOST} ${STREAMR_URL} ${TRACKER_URL}