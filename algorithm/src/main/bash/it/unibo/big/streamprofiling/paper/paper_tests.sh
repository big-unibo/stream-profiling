#!/bin/sh
container=""
# Check if the first parameter parameter exists and its content is "docker"
if [ "$1" = "docker" ]; then
    container="docker"
    # build the containers
    cd ../../../../../../../../../
    docker build -t algorithm -f DockerfileAlgorithm .
    docker build -t generator -f DockerfileGenerator .
    cd algorithm/src/main/bash/it/unibo/big/streamprofiling/paper/
fi

built="true"
generate_simulation=false
if [ "$2" = "true" ]; then
    generate_simulation=true
fi

# Output the value of the container variable (optional for debugging)
echo "Container is set to: $container"

#each script is called with 3 parameters:
# 1. container if docker execute with docker
# 2. generate_simulation if true generate the datasets
# 3. built if true the containers are also built
bash algoParameters.sh $container $generate_simulation $built
bash windowPeriodsNew.sh $container $generate_simulation $built
cd ..
bash full_sim_scenarios.sh $container $generate_simulation $built
