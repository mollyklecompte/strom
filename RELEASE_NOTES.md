# Release Notes for MVP#2

MVP#2 added or improved the following features from MVP#1:

- Added Kafka for real time asynchronous processing. The server now posts to a
Kafka topic which the engine ingests.

- Added an engine module, which ingests and processes data from Kafka.

- Added docker-compose.yml and container_ip.sh files to enable docker container
setup and teardown by script. container_pi.sh facilitates Docker outside of Docker
on Jenkins

- Added a start/stop shell script (start_services.sh) to start and stop the Docker
containers using the docker-compose script, start the server, start the engine, and
start pushing data to simulate a continuous data stream.

- Added stopwatch module to enable speed tests.
