#!/bin/sh
common_items=$1
shift
probability=$1
shift
lifetime="100000"
unlifetime="[0,0,0,0,0,0,0,0,0,-1]"
simulation_actions="[STATIC,STATIC,STATIC,STATIC,STATIC,STATIC,STATIC,STATIC,STATIC,STATIC]"
file_name="../stream-profiling/fadeout-common${common_items}-fixed${probability}/seeds" #give a simulation file name

cd ../common/
bash abstractSimulation.sh ${common_items} ${probability} ${lifetime} ${unlifetime} ${simulation_actions} ${file_name} "$@"