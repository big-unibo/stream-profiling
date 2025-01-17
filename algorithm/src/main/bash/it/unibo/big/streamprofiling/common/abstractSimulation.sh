#!/bin/sh
# parameters:
# - common_items
# - probability
# - lifetime
# - unlifetime
# - simulation_actions
# - file name
# - algorithms
seeds=10
seeds_values=10
#2000 records ==> 100000
window_duration=100000
#200 records ==> 10000
slide_duration=10000
duration=600000
##############################
common_items=$1
shift
probability=$1
shift
lifetime=$1
shift
unlifetime=$1
shift
simulation_actions=$1
shift
file_name=$1
shift
#############################
clusterings=("$@") # Rebuild the array with rest of arguments
cd ../../../../../../../../

echo "Simulation with common = ${common_items} and probability = ${probability}"

set -- 500 #1000 800 600 400 200 100 50 #posix notation
for f in "$@";  do
  echo "frequency in $f seeds $seeds"
  # parameters: number of seeds, frequency, simulation actions, number of seeds values, number of common items, life time, unlife time, probability, duration, fileName
  if [ -z "${file_name}" ]; then
    file_name="stream-profiling/seeds_f${f}_n${seeds}_values${seeds_values}_common${common_items}_duration${duration}_lifetime${lifetime}_unlifeTime${unlifetime}_simulationActions${simulation_actions}_probability${probability}"
  fi
  echo "file name $file_name"
  simulationFileNameTmp="${file_name}_SIM"
  simulationFileName="${simulationFileNameTmp}.csv"
  debugFolder="${simulationFileNameTmp}"
  echo "simulation $simulationFileName folder $debugFolder"
  rm -r "$debugFolder"
  mkdir -p "$debugFolder/logs"

  #2-simulation
  for ((i = 0; i < ${#clusterings[@]}; i++))
  do
      c=${clusterings[$i]}
      echo "clustering algorithm $c"
      # parameters: frequency, number of seeds, simulation filename, window duration, slide duration, clustering algorithm
      java -cp algorithm/build/libs/algorithm-0.1-all.jar it.unibo.big.streamprofiling.algorithm.app.AlgorithmApp $f $seeds "$simulationFileName" $window_duration $slide_duration "$c" > "${debugFolder}/logs/${c}_clustering.log" 2>&1
      echo "End simulation..."
  done
done