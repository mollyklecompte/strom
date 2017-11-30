#!/bin/bash
docker stop jenky-mongo && docker stop mariadb && docker stop zookafka
docker run --name mariadb -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=test -e MYSQL_USER=user -e MYSQL_PASSWORD=123 -d --rm -p 3306:3306 mariadb:latest
docker run --name jenky-mongo -d --rm mongo
docker run -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=172.17.0.5 --env ADVERTISED_PORT=9092 -d --rm --name zookafka spotify/kafka