#!/bin/sh
common_items=1
lifetime=10000000
unlifetime=0
simulation_actions="STATIC"
probability=0.9
file_name="stream-profiling/algoParameters/seeds" #give a simulation file name
# Define the array of values to iterate through
m_values=(5 10 25 50 100 200 300 400)
l_values=()
metric_values=("Silhouette" "SSE")
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
    algorithms+=("OMRkpp(20,$value)")
done
# Use the set command to transform the array into a set
set_algorithms=($(echo "${algorithms[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' '))

echo "${set_algorithms[@]}"

bash common/abstractSimulation.sh ${common_items} ${probability} ${lifetime} ${unlifetime} ${simulation_actions} ${file_name} "${set_algorithms[@]}"