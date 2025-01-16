#!/bin/sh

# the script receive a parameter that is "stream-analysis" or "stream-profiling"

# first build the container in DockerfileGraphs
docker build -t $1-graphs -f DockerfileGraphs .

# Create necessary directories if they don't exist
mkdir -p "$(pwd)/$1/graphs"

# Run the Docker container, mounting the directory and launching the script based on the parameter
if [ $1 = "stream-analysis" ]; then
    docker run -v /"$(pwd)/$1:/app/$1" $1-graphs algorithm/src/main/bash/it/unibo/big/streamanalysis/graphs-script.sh
else
    docker run -v /"$(pwd)/$1:/app/$1" $1-graphs algorithm/src/main/bash/it/unibo/big/streamprofiling/graphs-script.sh
fi