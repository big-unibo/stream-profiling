#!/bin/sh
cover_different_m=true
cover_different_l=false #other dataset (NOT NOW)
cover_different_metric=true
common_items=1
lifetime=10000000
unlifetime=0
simulation_actions="STATIC"
generate_simulation=$2 #true if want to generate seeds data
reset_previous_results=true  #true if want to reset previous simulation clustering results data
probability=0.9
file_name="stream-profiling/algoParameters/seeds" #give a simulation file name
container="$1" # - container if docker -> launch with docker, else locally
built="$3" # - if true the containers are also built
# Define the array of values to iterate through
if $cover_different_m ; then
  m_values=(5 10 25 50 100 200 300 400)
else
  m_values=(100)
fi

if $cover_different_l ; then
  l_values=(1 2 3 4 5 10 15 20 25 )
else
  l_values=()
fi

if $cover_different_metric ; then
  metric_values=("Silhouette" "SSE")
else
  metric_values=("Silhouette")
fi

# Define the initial array
algorithms=("FEACS(20,10,10,0.5)" "OMRkpp(20,Silhouette)")

# Loop through the values array and add each value to the end of the array
for value in "${m_values[@]}"
do
    algorithms+=("DSC(15,$value,Silhouette,CENTROID,false,-20,20,1.0,0.1,20)")
    algorithms+=("DSC(15,$value,Silhouette,CENTROID,true,-20,20,1.0,0.1,20)")
    algorithms+=("CSCS(15,$value,Silhouette,20)")
    algorithms+=("DSC(15,$value,SSE,CENTROID,false,-20,20,1.0,0.1,20)")
    algorithms+=("DSC(15,$value,SSE,CENTROID,true,-20,20,1.0,0.1,20)")
    algorithms+=("CSCS(15,$value,SSE,20)")
done

# Loop through the values array and add each value to the end of the array
for value in "${l_values[@]}"
do
    algorithms+=("DSC($value,1000,Silhouette,CENTROID,false,-20,20,1.0,0.1,20)")
    algorithms+=("DSC($value,1000,Silhouette,CENTROID,true,-20,20,1.0,0.1,20)")
    algorithms+=("CSCS($value,1000,Silhouette,20)")
    algorithms+=("DSC($value,1000,SSE,CENTROID,false,-20,20,1.0,0.1,20)")
    algorithms+=("DSC($value,1000,SSE,CENTROID,true,-20,20,1.0,0.1,20)")
    algorithms+=("CSCS($value,1000,SSE,20)")
done

# Loop through the values array and add each value to the end of the array
for value in "${metric_values[@]}"
do
    algorithms+=("DSC(15,100,$value,CENTROID,false,-20,20,1.0,0.1,20)")
    algorithms+=("DSC(15,100,$value,CENTROID,true,-20,20,1.0,0.1,20)")
    #algorithms+=("CSCS(15,100,$value)")
    algorithms+=("OMRkpp(20,$value)")
    #algorithms+=("AGNES(WardLinkage,$value)")
done
# Use the set command to transform the array into a set
set_algorithms=($(echo "${algorithms[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' '))

echo "${set_algorithms[@]}"

cd ../
bash abstractSimulation.sh ${container} ${built} ${common_items} ${probability} ${lifetime} ${unlifetime} ${simulation_actions} \
                         ${generate_simulation} ${reset_previous_results} ${file_name} "${set_algorithms[@]}"
