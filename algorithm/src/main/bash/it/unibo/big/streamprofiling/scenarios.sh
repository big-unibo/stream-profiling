#!/bin/sh

# Fixed parameters
common_items=1
probability=0.9

#!/bin/sh

# Fixed parameters
common_items=1
probability=0.9

# Define the algorithms
algorithms=(
    "FEACS(20,10,10,0.5)"
    "OMRkpp(20,Silhouette)"
    "OMRkpp(20,SSE)"
    "DSC(15,100,Silhouette,CENTROID,false,-20,20,1.0,0.1,20)"
    "DSC(15,100,Silhouette,CENTROID,true,-20,20,1.0,0.1,20)"
    "CSCS(15,300,Silhouette,20)"
    "CSCS(15,100,Silhouette,20)"
    "DSC(15,100,SSE,CENTROID,false,-20,20,1.0,0.1,20)"
    "DSC(15,100,SSE,CENTROID,true,-20,20,1.0,0.1,20)"
    "CSCS(15,300,SSE,20)"
    "CSCS(15,100,SSE,20)"
)

# Define a function to run all phases with the given parameters
run_phases() {
    local common_items=$1
    local probability=$2
    local algorithms=$3

    bash static.sh $common_items $probability "$algorithms"
    bash fadein.sh $common_items $probability "$algorithms"
    bash merge.sh $common_items $probability "$algorithms"
    bash split.sh $common_items $probability "$algorithms"
    bash fadeout.sh $common_items $probability "$algorithms"
    bash slide.sh $common_items $probability "$algorithms"
}

# Convert algorithms array to a space-separated string
algorithms_string="${algorithms[@]}"

# Run complete scenarios simulation
run_phases $common_items $probability "$algorithms_string"
