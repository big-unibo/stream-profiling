#!/bin/sh
# parameters:
# - container if docker -> launch with docker, else locally
# - built -> if true the containers are also built
# - common_items
# - probability
# - lifetime
# - unlifetime
# - simulation_actions
# - generate_simulation
# - reset_previous_results
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
container="$1"
echo "Run abstract simulation with $container"
shift
built="$1"
echo "Build containers $built"
shift
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
generate_simulation=$1
shift
reset_previous_results=$1
shift
file_name=$1
shift
#############################
clusterings=("$@") # Rebuild the array with rest of arguments
if [ "$container" = "docker" ]; then
  cd ../../../../../../../../
  # if $built == "true" skip this code
  if [ "$built" = "true" ]; then
    echo "Skip build containers"
  else
    docker build -t algorithm -f DockerfileAlgorithm .
    docker build -t generator -f DockerfileGenerator .
  fi
else
  source ./pythonSetup.sh
  cd ../../../../../../../../
  ./gradlew shadowJar
fi

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
  debugFolder="${simulationFileNameTmp}" #_window=${window_duration}_slide=${slide_duration}
  echo "simulation $simulationFileName folder $debugFolder"
  if $reset_previous_results ; then
    rm -r "$debugFolder"
  fi
  mkdir -p "$debugFolder/logs"

  #1-generation
  if $generate_simulation ; then
    if [ "$container" = "docker" ]; then
        docker run -v /${PWD}/stream-profiling:/app/stream-profiling generator java -cp generator/build/libs/generator-0.1-all.jar it.unibo.big.streamprofiling.generator.WriteSimulationFromGenerationKt $seeds $f $simulation_actions $seeds_values $common_items $lifetime $unlifetime $probability $duration $file_name > "${debugFolder}/logs/generator.log" 2>&1
    else
       java -cp generator/build/libs/generator-0.1-all.jar it.unibo.big.streamprofiling.generator.WriteSimulationFromGenerationKt $seeds $f $simulation_actions $seeds_values $common_items $lifetime $unlifetime $probability $duration $file_name > "${debugFolder}/logs/generator.log" 2>&1
    fi
    echo "Sleep for generation..."
    sleep 20s
  fi

  #2-simulation
  for ((i = 0; i < ${#clusterings[@]}; i++))
  do
      c=${clusterings[$i]}
      echo "clustering algorithm $c"
      # Use find to get a list of all files in the directory and its subdirectories
      if $reset_previous_results ; then
          echo "not change files"
      else
        files=$(find "$debugFolder" -type f)
        # Loop through each file in the list and remove all rows that contain the string
        for file in $files; do
              # Create a new file with suffix _new
              new_file="${file}_new"
              # Use sed to remove all rows that contain the string and write to new file
              sed "/$c/d" "$file" > "$new_file"
              # Replace original file with new file
              mv "$new_file" "$file"
        done
      fi

      # parameters: frequency, number of seeds, simulation filename, window duration, slide duration, clustering algorithm
      if [ "$container" = "docker" ]; then
        docker run -v /${PWD}/stream-profiling:/app/stream-profiling algorithm java -cp algorithm/build/libs/algorithm-0.1-all.jar it.unibo.big.streamprofiling.algorithm.app.AlgorithmApp $f $seeds "$simulationFileName" $window_duration $slide_duration "$c" > "${debugFolder}/logs/${c}_clustering.log" 2>&1
      else
        java -cp algorithm/build/libs/algorithm-0.1-all.jar it.unibo.big.streamprofiling.algorithm.app.AlgorithmApp $f $seeds "$simulationFileName" $window_duration $slide_duration "$c" > "${debugFolder}/logs/${c}_clustering.log" 2>&1
      fi
      echo "End simulation..."
  done
done