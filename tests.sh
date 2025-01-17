#!/bin/sh

docker build -t stream-profiling -f Dockerfile .

# run the container
docker run -v "$(pwd)/stream-profiling:/app/stream-profiling" stream-profiling