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
# Original array of window lengths
original_windows_lengths=(1000 10000 100000 1000000)
fixed_wl=10000
test_fixed_wl=true #true if want to test fixed window length
test_fixed_wp_percentage=true #true if want to test fixed window period percentage
fixed_pane_percentace=10
simulation_actions="STATIC"
probability=0.9
ws=1699389308172 # to change in case of different input file and different window start
# Multiply each value in the array by 50
windows_lengths=()
windows_periods=()
windows_clusterings=()
fixed_length_test=$((fixed_wl*50))
clusterings=("DSC(15,100,SSE,CENTROID,false,-20,20,1.0,0.1,20)" "CSCS(15,100,SSE,20)" "FEACS(20,10,10,0.5)" "DSC(15,100,SSE,CENTROID,true,-20,20,1.0,0.1,20)" "OMRkpp(20,SSE)")

# populate arrays for each key in windows_lengths
for input_lenght in "${original_windows_lengths[@]}"; do
    key=$((input_lenght * 50))
    if [[ "$key" == "$fixed_length_test" ]] && [ "$test_fixed_wl" == true ]; then
        for c in "${clusterings[@]}"; do
            if [[ "$c" == *OMRk* || "$c" == *AGNES* ]]; then
               windows_lengths+=($key)
               windows_periods+=($key)
               windows_clusterings+=($c)
            else
              period_add=$((key / 100))
              x=$((25 * input_lenght / 10))
              while [ $x -le "$key" ]; do
                #add only if multiple
                if [[ $((key % x)) -eq 0 ]]; then
                  windows_lengths+=($key)
                  windows_periods+=($x)
                  windows_clusterings+=($c)
                fi
                x=$(( x + period_add ))
              done
            fi
        done
    elif [ "$test_fixed_wp_percentage" == true ]; then
        for c in "${clusterings[@]}"; do
          windows_lengths+=($key)
          windows_periods+=($((key / fixed_pane_percentace)))
          windows_clusterings+=($c)
        done
    fi
done

# Initialize the maximum value to the first element of the array
max_wl=${windows_lengths[0]}
# Iterate through the array elements and update the maximum value if needed
for value in "${windows_lengths[@]}"; do
    if [ "$value" -gt "$max_wl" ]; then
        max_wl="$value"
    fi
done
max_duration_wp=$((max_wl + (max_wl/fixed_pane_percentace) * 10)) #10 iterations
max_wl_fixed=$((fixed_length_test * 3))
max_wl=$((max_duration_wp > max_wl_fixed ? max_duration_wp : max_wl_fixed))
duration=$((test_fixed_wl ? (test_fixed_wp_percentage ? max_wl : max_wl_fixed) : max_duration_wp))
##############################
common_items=1
max_lifetime_wp=$((max_duration_wp + 100000))
max_fixed_duration=$((fixed_length_test * 4))
max_lifetime=$((max_lifetime_wp > max_fixed_duration ? max_lifetime_wp : max_fixed_duration))
lifetime=$((test_fixed_wl ? (test_fixed_wp_percentage ? max_lifetime : max_fixed_duration) : max_lifetime_wp))
unlifetime=0
echo "lifetime $lifetime and duration $duration"
finalwe=$((ws + duration))
file_name="stream-profiling/windowTest/seeds" #give a simulation file name
#############################

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

  echo "simulation.."
  #2-simulation
  # Get the length of one of the arrays (assuming both arrays have the same length)
  length=${#windows_lengths[@]}
  # Iterate through the arrays using the same index
  for ((i = 0; i < length; i++)); do
      wl="${windows_lengths[i]}"
      wp="${windows_periods[i]}"
      c="${windows_clusterings[i]}"
      echo "Simulation with length $wl and period $wp for $c"
      echo "records length = $((wl / 50)) and records period = $((wp / 50))"
      # parameters: frequency, number of seeds, simulation filename, window duration, slide duration, clustering algorithm, debug, window_start, window_end
      if [[ "$c" == *OMRk* || "$c" == *AGNES* ]]; then
          if [ "$wl" -eq 50000 ]; then
            iterations=40
          else
            iterations=3
          fi
      elif [ "$wl" -eq 50000 ]; then
          iterations=50
      elif [ "$wl" -eq 50000000 ]; then
          iterations=5
      else
          iterations=10
      fi
      we=$((wl + ws + wp * iterations)) #for reduce the number of iterations
      # Compare and assign the minimum
      we=$((we < finalwe ? we : finalwe))
      java -cp algorithm/build/libs/algorithm-0.1-all.jar it.unibo.big.streamprofiling.algorithm.app.AlgorithmApp $f $seeds "$simulationFileName" $wl $wp "$c" "false" $ws $we > "${debugFolder}/logs/${c}_clustering_wp${wp}_wl${wl}.log" 2>&1
  done
  echo "End simulation..."
done