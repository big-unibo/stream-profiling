#!/bin/sh

# Fixed parameters
common_items=1
probability=0.9

# Define the algorithms
set_algorithms=(
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

bash scenarios/static.sh $common_items $probability "${set_algorithms[@]}"
bash scenarios/fadein.sh $common_items $probability "${set_algorithms[@]}"
bash scenarios/merge.sh $common_items $probability "${set_algorithms[@]}"
bash scenarios/split.sh $common_items $probability "${set_algorithms[@]}"
bash scenarios/fadeout.sh $common_items $probability "${set_algorithms[@]}"
bash scenarios/slide.sh $common_items $probability "${set_algorithms[@]}"
