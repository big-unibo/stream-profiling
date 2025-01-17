#!/bin/sh
common_items=$1
shift
probability=$1
shift

lifetime="600000"
unlifetime="[0,0,0,0,0,0,0,0,0,100000]"
simulation_actions="[STATIC,STATIC,STATIC,STATIC,STATIC,STATIC,STATIC,STATIC,STATIC,FADEIN]"
file_name="stream-profiling/fadein-common${common_items}-fixed${probability}/seeds" #give a simulation file name

cd ../common/
bash abstractSimulation.sh ${common_items} ${probability} ${lifetime} ${unlifetime} ${simulation_actions} ${file_name} "$@"