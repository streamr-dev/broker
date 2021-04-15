FROM node:14-buster as build
WORKDIR /usr/src/broker
COPY . .
RUN npm ci --only=production
RUN npm run build

FROM node:14-buster-slim
RUN apt-get update && apt-get install --assume-yes --no-install-recommends curl \
	&& apt-get clean \
	&& rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/src/broker /usr/src/broker
WORKDIR /usr/src/broker

# Make ports available to the world outside this container
EXPOSE 30315
# WebSocket
EXPOSE 8890
# HTTP
EXPOSE 8891
# MQTT
EXPOSE 9000

ENV LOG_LEVEL=info
ENV CONFIG_FILE configs/docker-1.env.json
ENV STREAMR_URL http://127.0.0.1:8081/streamr-core

CMD node bin/broker.js ${CONFIG_FILE} --streamrUrl=${STREAMR_URL}
