#!/bin/sh

common_items=$1
shift
probability=$1
shift

lifetime="500000"
unlifetime="0"
simulation_actions="[STATIC,STATIC,STATIC,STATIC,STATIC,STATIC,STATIC,STATIC,MERGE:m1,MERGE:m1]"
file_name="stream-profiling/merge-common${common_items}-fixed${probability}/seeds" #give a simulation file name

bash common/abstractSimulation.sh ${common_items} ${probability} ${lifetime} ${unlifetime} ${simulation_actions} ${file_name} "$@"
