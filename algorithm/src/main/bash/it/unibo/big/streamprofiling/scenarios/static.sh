#!/bin/sh

common_items=$1
shift
probability=$1
shift

lifetime="100000"
unlifetime=0
simulation_actions="STATIC"
file_name="stream-profiling/static-common${common_items}-fixed${probability}/seeds" #give a simulation file name

cd ../common/
bash abstractSimulation.sh ${common_items} ${probability} ${lifetime} ${unlifetime} ${simulation_actions} ${file_name} "$@"