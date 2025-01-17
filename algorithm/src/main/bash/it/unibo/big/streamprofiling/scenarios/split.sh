#!/bin/sh

common_items=$1
shift
probability=$1
shift
generate_simulation=$1
shift
reset_previous_results=$1
shift

lifetime="500000"
unlifetime="0"
simulation_actions="[STATIC,STATIC,STATIC,STATIC,STATIC,STATIC,STATIC,STATIC,STATIC,SPLIT:split-1]"
file_name="../stream-profiling/split-common${common_items}-fixed${probability}/seeds" #give a simulation file name

cd ../common/
bash abstractSimulation.sh ${common_items} ${probability} ${lifetime} ${unlifetime} ${simulation_actions} ${file_name} "$@"