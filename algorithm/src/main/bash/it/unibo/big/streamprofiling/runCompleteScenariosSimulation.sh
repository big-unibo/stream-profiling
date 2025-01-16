#!/bin/sh
#parameters
# - container if docker -> launch with docker, else locally
# - built -> if true the containers are also built
# - common_items
# - probability
# - generate_simulation
# - reset_previous_results
# - algorithms

container=$1
shift
built=$1
shift
common_items=$1
shift
probability=$1
shift
generate_simulation=$1
shift
reset_previous_results=$1
shift

bash static.sh $container $built $common_items $probability $generate_simulation $reset_previous_results "$@"
bash fadein.sh $container $built $common_items $probability $generate_simulation $reset_previous_results "$@"
bash merge.sh $container $built $common_items $probability $generate_simulation $reset_previous_results "$@"
bash split.sh $container $built $common_items $probability $generate_simulation $reset_previous_results "$@"
bash fadeout.sh $container $built $common_items $probability $generate_simulation $reset_previous_results "$@"
bash slide.sh $container $built $common_items $probability $generate_simulation $reset_previous_results "$@"