#!/bin/bash
echo "Let's get things going"
### startup docker containers detached ###
docker-compose -f docker/support_containers/docker-compose.yml up -d
sleep 4
### starup flask server in background ###
cd python/Strom
python -m strom.strom-api.api.server 2>&1 > /dev/null & echo $!
cd ../..
sleep 4
### startup engine in background ###
echo "All aboard the Engine class!"
### start data pushing script ###
cli/data_poster/post_data.sh
