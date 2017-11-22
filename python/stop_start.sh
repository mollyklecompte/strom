#!/bin/bash
docker stop jenky-mongo && docker rm jenky-mongo && docker stop mariadb
docker run --name mariadb -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=test -e MYSQL_USER=user -e MYSQL_PASSWORD=123 -d --rm -p 3306:3306 mariadb:latest && docker run --name jenky-mongo -d mongo
