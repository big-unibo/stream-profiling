#!/bin/sh

docker build -t stream-profiling -f Dockerfile .
mkdir -p stream-profiling
# run the container
docker run -v /${PWD}/stream-profiling:/app/stream-profiling stream-profiling bash -c "
                                                                                cp -r /app/stream-profiling-data/* /app/stream-profiling &&
                                                                                rm -r /app/stream-profiling-data &&
                                                                                bash /app/run_tests.sh
                                                                              "