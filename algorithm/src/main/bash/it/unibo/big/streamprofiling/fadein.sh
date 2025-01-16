#!/bin/sh
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

lifetime="600000"
unlifetime="[0,0,0,0,0,0,0,0,0,100000]"
simulation_actions="[STATIC,STATIC,STATIC,STATIC,STATIC,STATIC,STATIC,STATIC,STATIC,FADEIN]"
file_name="stream-profiling/fadein-common${common_items}-fixed${probability}/seeds" #give a simulation file name

bash abstractSimulation.sh ${container} ${built} ${common_items} ${probability} ${lifetime} ${unlifetime} ${simulation_actions} \
                         ${generate_simulation} ${reset_previous_results} ${file_name} "$@"