#!/bin/bash
## Script for preparing smoke test
sudo ifconfig docker0 10.200.10.1/24
## Get Streamr Docker dev
git clone https://github.com/streamr-dev/streamr-docker-dev.git
## Switch out image for local one
sed -i "s#$OWNER/$IMAGE_NAME:dev#$OWNER/$IMAGE_NAME\:taggit#g" $TRAVIS_BUILD_DIR/streamr-docker-dev/docker-compose.override.yml
## Start up services needed
$TRAVIS_BUILD_DIR/streamr-docker-dev/streamr-docker-dev/bin.sh  start --except tracker-1 --except tracker-2 --except tracker-3 --except broker-node-no-storage-2 --except broker-node-storage-1 --wait

## Wait for the service to come online and test
wait_time=10;
for (( i=0; i < 5; i=i+1 )); do
    curl http://localhost:8791/api/v1/volume;
    res=$?;
    if test "$res" != "0"; then
        echo "Attempting to connect to broker retrying in $wait_time seconds";
        sleep $wait_time;
        wait_time=$(( 2*wait_time )) ;
    else
        break;
    fi;
done;
set -e
curl http://localhost:8791/api/v1/volume
