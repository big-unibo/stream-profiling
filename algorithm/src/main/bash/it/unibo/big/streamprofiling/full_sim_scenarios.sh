#!/bin/sh
container="$1"
built="$3"
generate_simulation=$2
bash runCompleteScenariosSimulation.sh $container $built 1 0.9 $generate_simulation true  "FEACS(20,10,10,0.5)" "OMRkpp(20,Silhouette)" "OMRkpp(20,SSE)" \
   "DSC(15,100,Silhouette,CENTROID,false,-20,20,1.0,0.1,20)" "DSC(15,100,Silhouette,CENTROID,true,-20,20,1.0,0.1,20)" "CSCS(15,300,Silhouette,20)" "CSCS(15,100,Silhouette,20)" \
   "DSC(15,100,SSE,CENTROID,false,-20,20,1.0,0.1,20)" "DSC(15,100,SSE,CENTROID,true,-20,20,1.0,0.1,20)" "CSCS(15,300,SSE,20)" "CSCS(15,100,SSE,20)"
