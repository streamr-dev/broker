FROM ubuntu:16.04

# Set the working directory to /app
WORKDIR /app

# Copy app code
COPY . /app

RUN apt-get update
RUN apt-get -qq upgrade
RUN apt-get install -y build-essential curl git
RUN curl -sL https://deb.nodesource.com/setup_10.x | bash
RUN apt-get install -y nodejs
RUN node --version
RUN npm ci

# Make ports available to the world outside this container
EXPOSE 30315
# WebSocket
EXPOSE 8890
# HTTP
EXPOSE 8891
# MQTT
EXPOSE 9000

ENV DEBUG=streamr:logic:*
ENV CONFIG_FILE configs/docker-1.env.json
ENV STREAMR_URL http://127.0.0.1:8081/streamr-core

CMD node app.js ${CONFIG_FILE} --streamrUrl=${STREAMR_URL}
