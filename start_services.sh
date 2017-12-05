#!/bin/bash
echo "Let's get things going"
### startup docker containers detached ###
docker-compose -f docker/support_containers/docker-compose.yml up -d
sleep 10 # wait for mariadb to start up
### starup flask server in background ###
cd python/Strom
python -m strom.strom-api.api.server & echo $!
cd ../..
sleep 2
### startup engine in background ###
echo "All aboard the Engine class!"
### start data pushing script ###
cd cli/data_poster
./post_data.sh
