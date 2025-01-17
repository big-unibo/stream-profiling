#!/bin/sh

docker build -t stream-profiling -f Dockerfile .
mkdir -p stream-profiling
# run the container
docker run -v /${PWD}/stream-profiling:/app/stream-profiling stream-profiling #check the mount