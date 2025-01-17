# Stream-profiling
This repository contains the code for the paper "Dynamic Stream Clustering for Real-Time Schema Profiling with DSC+".

Project structure:
- `generator/` -- generators of stream data
- `algorithm/` -- algorithms implementations

# Run the code
Docker is required to run the tests.
Open docker and from a terminal, if you use Windows use Git Bash, run the following commands:
```bash
./tests.sh
```

## Detail of the tests
Each dataset is computed considering the following clustering algorithms:
- DSC+: the algorithm proposed in the paper
- OMRk++: consists in running k-means++ for multiple values of *k* and choosing the best solution according to the well-known *elbow method* approach
- DSC: the algorithm proposed in the [Streaming Approach to Schema Profiling](https://link.springer.com/chapter/10.1007/978-3-031-42941-5_19)
- CSCS: the algorithm proposed in [Efficient Data Stream Clustering With Sliding Windows Based on Locality-Sensitive Hashing](https://ieeexplore.ieee.org/document/8501907/)
- FEAC-S: the algorithm proposed in [An evolutionary algorithm for clustering data streams with a variable number of clusters](https://www.sciencedirect.com/science/article/pii/S0957417416304985)

The tests runned consists on the script [run_tests](algorithm/src/main/bash/it/unibo/big/streamprofiling/run_tests.sh) that runs the following sub-tests:
- [algoParameters](algorithm/src/main/bash/it/unibo/big/streamprofiling/algoParameters.sh) that runs the clustering algorithms on the static dataset changing the DSC's algorithms parameters.
- [windowPeriodsParameters](algorithm/src/main/bash/it/unibo/big/streamprofiling/windowParameters.sh) that runs the clustering algorithms on the static dataset changing the window parameters.
- [scenarios](algorithm/src/main/bash/it/unibo/big/streamprofiling/scenarios.sh) that runs the clustering algorithms on each scenario (static, fadein, fadeout, slide, split, merge).
- [graphs](algorithm/src/main/bash/it/unibo/big/streamprofiling/graphs.sh) that generates the graphs of the results and the csv files of the table that is in the paper.
  It saves the graphs in the graphs folder.

For the first three tests, the execution of the test is given by the class [AlgorithmApp](algorithm/src/main/scala/it/unibo/big/streamprofiling/algorithm/app/AlgorithmApp.scala).
This class requires the following parameters:
- frequency of the window data (used only for debug purposes)
- number of clusters (used only for debug purposes and calculate the AMI)
- name of the file where the data is stored
- window duration in milliseconds
- window period in milliseconds
- clustering algorithm configuration to execute
- debug true if you want to print the debug information
- window start timestamp: an optional that tells the timestamp of the first window (otherwise the first record timestamp is used)
- window end timestamp: an optional that tells the timestamp of the last window (otherwise the last record timestamp is used)
- random init: flag used for reproducibility of the results

Default parameters are taken from [application.conf](algorithm/src/main/resources/application.conf).

### Clustering algorithms name and parameters
The clustering algorithms configurations are defined in [ClusteringAlgorithmsImplementations](algorithm/src/main/scala/it/unibo/big/streamprofiling/algorithm/execution/ClusteringAlgorithmsImplementations.scala).
When we pass an algorithm to the [AlgorithmApp](algorithm/src/main/scala/it/unibo/big/streamprofiling/algorithm/app/AlgorithmApp.scala) class, we pass a string that is parsed.
The format of the string follows the following patterns (with = we indicate default parameter values):
- **DSC** and **DSC+**
  - DSC(l=15,m=100,Metric=Silhouette,CENTROID,useHeuristicForReduceCoreset=false,mergeTau=-3,overlapTau=0.15,splitTau=30,insertDiff,elbowSensitivity=1)
      - insertDiff not used now
      - CENTROID is reduce corest metric (CENTROID|STANDARD|HASH_LENGTH|HAMMING|JACCARD) STANDARD is manhattan as in CSCS
      - DSC(l,m,Metric) without local reclustering rules, just incremental
      - DSC(l,m,Metric,ReduceCorestMetric), just incremental
      - DSC(l,m,Metric,CENTROID,mergeTau,overlapTau,splitTau,insertDiff)
- **OMRk++**
  - OMRkpp(Metric=Silhouette) --> launch OMRkpp from 2 to sqrt record, using the metric (SSE|Silhouette)
      - OMRkpp(kMax,Metric)
      - OMRkpp(kMin, kMax,Metric)
- **CSCS**
  - CSCS(l=15,m=100/300,Metric=Silhouette) Reduce corest with euristic and manatthan
      - CSCS(l,m,tol,Metric) #tolerance of LSH, if not provided calculated by predefined process with a data sample
      - CSCS(l,m,Metric,ReduceCorestMetric)
      - CSCS(l,m,tol,Metric,ReduceCorestMetric)
- **FEAC-S**
  - FEACS(numberOfPopulation=10,kmeansIter=10,percentageMutation=0.5)
      - FEACS(numberOfPopulation,kmeansIter,percentageMutation,startingNumberOfCluster) startingNumberOfCluster for start with a not random number of clusters

### Outputs
## Results Structure
The results are structured as follows. In the [stream-profiling](stream-profiling) folder, there are subfolders with the test results. Specifically:

- For each type of test, there is a folder containing:
    - A file `seeds_SIM.csv` which is the dataset.
    - A folder `seeds_SIM` containing CSV files that represent the test results:
        - **frequency_statistics.csv**: This is the main file with statistics related to the clustering result of a window.
          Columns:
            - t: timestamp of the pane
            - window_start: timestamp of the start of the window
            - window_end: timestamp of the end of the window
            - window_period: the difference between window_end and window_start
            - k: number of clusters
            - cluster-type: the algorithm used with parameters
            - clustering-parameters: the name of the algorithm parameters separated by commas
            - records: number of records in the pane
            - elapsed-time: time taken for clustering
            - sse: SSE of the clustering result
            - silhouette: Simplified Silhouette of the clustering
            - AMI: Adjusted Mutual Information of the clustering considering cluster labels
            - ARI: Adjusted Rand Index of the clustering considering cluster labels
            - seeds: actual number of clusters (counted considering the labels)
            - data_frequency: how often a new schema is generated, the same for all data in the pane (used for debugging and graphs where it's known to be a fixed value)
            - windowLength: window length
            - windowPeriod: window period
            - totalTime: total execution time, different from elapsed-time because if multiple k values are tested, it takes the time to find the best one. Generally, this value is used instead of elapsed-time.
            - timeForShapeResult: time to convert the algorithm's clustering result to the "standard" format where each input record has a label.
            - metric: metric used to select the best result (if applicable)
            - NUMBER_OF_CHANGE_DICTIONARY: number of times the dictionary was changed, applies to CSCS which does not work on DUD
            - NUMBER_OF_REDUCE_CORESET: number of coreset reductions (applies to CSCS, DSC, and DSC+)
            - NUMBER_OF_INPUT_BUCKETS: number of input buckets (applies to CSCS, DSC, and DSC+)
            - NUMBER_OF_RECLUSTERING: number of reclusterings (applies to FEACS, CSCS, DSC, and DSC+)
            - NUMBER_OF_BUCKETS: number of output buckets (applies to CSCS, DSC, and DSC+)
            - TIME_FOR_CLUSTERING: time for clustering (applies to FEACS, CSCS, DSC, and DSC+)
            - TIME_FOR_CHANGE_DICTIONARY: time to change the dictionary (applies to CSCS)
            - TIME_FOR_REMOVE_OLD_RECORDS: time to remove old records (applies to FEACS, CSCS, DSC, and DSC+)
            - TIME_FOR_FIND_CLUSTER_TO_SPLIT_MERGE: time to find clusters to split or merge (applies to DSC and DSC+)
            - TIME_FOR_MERGE: time to merge clusters (applies to DSC and DSC+)
            - TIME_FOR_SPLIT: time to split clusters (applies to DSC and DSC+)
            - TIME_FOR_UPDATE_HASH_TABLE_START: time to update the hash table (applies to CSCS, DSC, and DSC+)
            - TIME_FOR_WHOLE_RECLUSTERING: time to complete the entire reclustering process (applies to FEACS, CSCS, DSC, and DSC+)
            - TIME_FOR_GENERATE_NEW_WINDOW: time to transition to the new window in two-phase algorithms (applies to CSCS, DSC, and DSC+)
            - TIME_FOR_REDUCE_CORESET: time to reduce the coreset (applies to CSCS, DSC, and DSC+)
            - TIME_FOR_UPDATE_RESULT: time to update the result by inserting an element into the nearest cluster (applies to FEAC-S, CSCS, DSC, and DSC+)
            - TIME_FOR_UPDATE_DATA_STRUCTURE: time to update the data structure (applies to FEACS, CSCS, DSC, and DSC+)
            - TIME_FOR_TEST_RESULT: time to test the result (KL divergence, check for splits or merges, FEAC PH test, etc., generally tests to see if the clustering should change) (applies to FEACS, CSCS, DSC, and DSC+)
            - NUMBER_OF_FIND_CLUSTER_TO_SPLIT_MERGE: number of attempts to find clusters to split or merge (applies to DSC and DSC+)
            - TIME_FOR_CALCULATE_FOR_SPLIT_OPERATION: time to calculate if a split should occur (applies to DSC and DSC+)
            - TIME_FOR_CALCULATE_FOR_MERGE_OPERATION: time to calculate if a merge should occur (applies to DSC and DSC+)
            - TIME_FOR_UPDATE_CLUSTERS: time to update the clusters (applies to CSCS, DSC, and DSC+)
            - TIME_FOR_CALCULATE_DISTANCES_BUCKET_CLUSTERS: time to calculate distances between buckets and clusters (applies to CSCS, DSC, and DSC+)
            - TIME_FOR_ASSIGN_HASH: time to assign the hash (applies to CSCS, DSC, and DSC+)
        - **2phases_clusters.csv**: For two-phase algorithms (CSCS, DSC, and DSC+), contains the clusters:
            - t: timestamp of the pane
            - window_start: timestamp of the start of the window
            - window_end: timestamp of the end of the window
            - clusterIndex: the cluster index
            - centroid: the cluster centroid
            - moreFrequentSeed: the most frequent seed
            - numberOfElementsWithSeed: the number of elements with that seed
            - AVG(sse): average SSE in the cluster
            - AVG(dist): average distance in the cluster
            - radius: the cluster radius
            - numberOfValues: the number of schemas in the cluster
            - clusteringName: the name of the algorithm with parameters
        - **2phases_clusters_distances.csv**: Distances between pairs of clusters for two-phase algorithms (CSCS, DSC, and DSC+):
            - t: timestamp of the pane
            - window_start: timestamp of the start of the window
            - window_end: timestamp of the end of the window
            - clusterIndexX: index of cluster X
            - clusterIndexY: index of cluster Y
            - centroidX: centroid of cluster X
            - centroidY: centroid of cluster Y
            - distance: distance between the two clusters
            - clusteringName: the name of the algorithm with parameters
            - distanceX: average distance of elements in cluster X
            - distanceMinY: minimum distance between an element in cluster X and the centroid of cluster Y
            - distanceY: average distance of elements in cluster Y
            - distanceMinX: minimum distance between an element in cluster Y and the centroid of cluster X
        - **2phases_large_operations_times.csv**: Times for operations like split, merge, clustering, and reclustering for two-phase algorithms (CSCS, DSC, and DSC+):
            - operation: the operation performed (SPLIT_OP, MERGE_OP, CLUSTERING, CLUSTERING_FROM_K)
            - numberOfElements: number of elements
            - elapsedTime: time taken for the operation
            - clusteringName: the name of the algorithm with parameters
        - **2phases_operations_debug.csv**: Operations performed by DSC and DSC+ over time:
            - algorithm: the algorithm used with parameters
            - t: timestamp of the pane
            - windowStart: timestamp of the start of the window
            - windowEnd: timestamp of the end of the window
            - operation: the operation performed (SPLIT, FADEOUT, MERGE, INSERT)
            - c1: centroid of cluster 1
            - c2: centroid of cluster 2 (if applicable)
            - zScore: z-score value (if the operation is SPLIT or MERGE)
            - RadiiVsDistances: cluster radius divided by distance (if the operation is MERGE)
            - numberOfNewClusters: number of new clusters created
            - opSuccess: whether the operation was successful
        - **dsc_zscore_scattering.csv**: For DSC and DSC+, values for scattering:
            - algorithm: the algorithm used with parameters
            - t: timestamp of the pane
            - windowStart: timestamp of the start of the window
            - windowEnd: timestamp of the end of the window
            - value: the reference cluster centroid
            - scattering: scattering value
            - zScoreScattering: z-score of scattering
        - **dsc_zscore_separation.csv**: For DSC and DSC+, values for separation:
            - algorithm: the algorithm used with parameters
            - t: timestamp of the pane
            - windowStart: timestamp of the start of the window
            - windowEnd: timestamp of the end of the window
            - value1: centroid of the first reference cluster
            - value2: centroid of the second reference cluster
            - zScoreSeparation: z-score value of separation
            - separation: separation value
            - overlappingZScores: z-score of the sum of scatterings divided by separation
            - overlapping_sumscattering_dividedby_distance: sum of two scatterings divided by the distance between the clusters

By default, tests will delete this folder.
The graphs that are in the paper are saved in the folder `stream-profiling/graphs/`.
The table with the result metrics that is in the paper is saved in the file `stream-profiling/tab_synthetic.csv`.

# Package structure

## Algorithm
The [algorithm](algorithm/src/main/scala/it/unibo/big/streamprofiling/algorithm) folder contains all the necessary classes.

- [app](algorithm/src/main/scala/it/unibo/big/streamprofiling/algorithm/app) contains the classes for running simulations, including the main class [AlgorithmApp](algorithm/src/main/scala/it/unibo/big/streamprofiling/algorithm/app/AlgorithmApp.scala).
- [execution](algorithm/src/main/scala/it/unibo/big/streamprofiling/algorithm/execution) contains utilities for executing algorithms.
    - The object [ClusteringAlgorithms](algorithm/src/main/scala/it/unibo/big/streamprofiling/algorithm/execution/ClusteringAlgorithms.scala) defines the execution of various algorithms.
    - The object [ClusteringAlgorithmsImplementations](algorithm/src/main/scala/it/unibo/big/streamprofiling/algorithm/execution/ClusteringAlgorithmsImplementations.scala) defines the parameters of the algorithms and their parsing.
- [implementation](algorithm/src/main/scala/it/unibo/big/streamprofiling/algorithm/implementation) contains the implementations of the algorithms and the necessary data structures.
    - [kmeans](algorithm/src/main/scala/it/unibo/big/streamprofiling/algorithm/implementation/kmeans) contains the implementation of the k-means algorithm.
        - The implementation starts from a generic version that is specialized for schemas in the file [KMeansOnSchemas](algorithm/src/main/scala/it/unibo/big/streamprofiling/algorithm/implementation/kmeans/KMeansOnSchemas.scala), particularly in the `predictCentroid` method (used in OMRk++).
    - [incremental](algorithm/src/main/scala/it/unibo/big/streamprofiling/algorithm/implementation/incremental) contains the implementation of incremental algorithms.
        - The interface (trait) [IncrementalClustering](algorithm/src/main/scala/it/unibo/big/streamprofiling/algorithm/implementation/incremental/IncrementalClustering.scala) defines the common methods for all incremental algorithms.
        - [twophases](algorithm/src/main/scala/it/unibo/big/streamprofiling/algorithm/implementation/incremental/twophases) contains the implementation of two-phase incremental algorithms.
            - Here, [Debug2PhasesAlgorithmsUtils](algorithm/src/main/scala/it/unibo/big/streamprofiling/algorithm/implementation/incremental/twophases/Debug2PhasesAlgorithmsUtils.scala) contains debug information for the two-phase algorithms.
            - [AbstractTwoPhasesClustering](algorithm/src/main/scala/it/unibo/big/streamprofiling/algorithm/implementation/incremental/twophases/AbstractTwoPhasesClustering.scala) provides the abstract implementation that combines both CSCS and DSC, which are 
               specialized by [CoreSetBasedClusteringWithSlidingWindows](algorithm/src/main/scala/it/unibo/big/streamprofiling/algorithm/implementation/incremental/twophases/CSCS/CoreSetBasedClusteringWithSlidingWindows.scala) (for CSCS) and [DynamicStreamClustering](algorithm/src/main/scala/it/unibo/big/streamprofiling/algorithm/implementation/incremental/twophases/DSC/DynamicStreamClustering.scala) (for DSC's) respectively.
        - FEAC-S is implemented in the class [FeacStreamAlgorithm](algorithm/src/main/scala/it/unibo/big/streamprofiling/algorithm/implementation/incremental/feacstream/FeacStreamAlgorithm.scala).
        - [groupfeature](algorithm/src/main/scala/it/unibo/big/streamprofiling/algorithm/implementation/incremental/groupfeature) contains the implementation of the group feature structure for incremental clustering.
- [utils](algorithm/src/main/scala/it/unibo/big/streamprofiling/algorithm/utils) contains utility classes.
    - Among them, the object [DebugWriter](algorithm/src/main/scala/it/unibo/big/streamprofiling/algorithm/utils/DebugWriter.scala) defines methods for writing to debug files.
    - The object [Settings](algorithm/src/main/scala/it/unibo/big/streamprofiling/algorithm/utils/Settings.scala) defines default parameters from a configuration file "application.conf" located in the resources folder.
- [Metrics](algorithm/src/main/scala/it/unibo/big/streamprofiling/algorithm/Metrics.scala) contains the definition of metrics used to evaluate the results of clustering.

## Generator
The [generator](generator/src/main/kotlin/it/unibo/big/streamprofiling/generator) folder contains all the necessary classes.

- [WriteSimulationFromGeneration](generator/src/main/kotlin/it/unibo/big/streamprofiling/generator/WriteSimulationFromGeneration.kt) contains the main class for generating datasets.
- [Constants](generator/src/main/kotlin/it/unibo/big/streamprofiling/generator/Constants.kt) handles command-line parameters.
- [seed](generator/src/main/kotlin/it/unibo/big/streamprofiling/generator/seed) contains all the logic for managing seeds and generating datasets.
    - [Generation](generator/src/main/kotlin/it/unibo/big/streamprofiling/generator/seed/Generation.kt) contains the dataset generation logic.

# The datasets
The datasets used here are available at [https://big.csr.unibo.it/downloads/stream-profiling/](https://big.csr.unibo.it/downloads/stream-profiling/).

## Generator
Datasets are generated using the `generator/` module.
For use the generator (not needed for reproduce the results) you can run the following command:
```bash
docker build -t stream-profiling-generator -f DockerfileGenerator .
docker run -v /${PWD}/stream-profiling:/app/stream-profiling stream-profiling-generator java -cp generator/build/libs/generator-0.1-all.jar it.unibo.big.streamprofiling.generator.WriteSimulationFromGenerationKt $seeds $f $simulation_actions $seeds_values $common_items $lifetime $unlifetime $probability $duration $file_name > "${debugFolder}/logs/generator.log" 2>&1
```

The generator use the [WriteSimulationFromGeneration](generator/src/main/kotlin/it/unibo/big/streamprofiling/generator/WriteSimulationFromGeneration.kt) class.
Parameter parsing is managed by [Constants.getParameters](generator/src/main/kotlin/it/unibo/big/streamprofiling/generator/Constants.kt).
It requires the following parameters:
- **Number of seeds (clusters)**: Specifies the number of clusters in the dataset.
- **Frequency**: A single value for all seeds or different values for each seed. For multiple values, provide a comma-separated list enclosed in `[` and `]`.
- **Actions in the dataset**: Either a single value for all seeds or different values for each seed, provided as a comma-separated list enclosed in `[` and `]`. Actions can include:
    - `STANDARD`
    - `FADEIN`
    - `MERGE:<newClusterName>` (e.g., merging into a new cluster)
    - `SPLIT:<newClusterName>` (e.g., splitting into a new cluster)
    - `SLIDE`

  For examples of settings, refer to the files invoked in the script [scenarios.sh](algorithm/src/main/bash/it/unibo/big/streamprofiling/scenarios.sh).
- **Attributes per seed**: Specifies the number of attributes for each seed. This can be a single value for all seeds or different values for each seed, provided as a comma-separated list enclosed in `[` and `]`.
- **Number of common values for the seeds**: Indicates the number of shared values among the seeds.
- **Life time**: Duration (in milliseconds) for which the seed remains active. A single value for all seeds or different values for each seed, provided as a comma-separated list enclosed in `[` and `]`.
- **Unlife time**: Duration (in milliseconds) for which the seed remains inactive before becoming active again. If set to `-1`, the seed never becomes active again. A single value for all seeds or different values for each seed, provided as a comma-separated list enclosed in `[` and `]`.
- **Exit probability for the seed**: A single value for all seeds or different values for each seed. For multiple values, provide them as an array (e.g., `[0.1,0.2,0.3]` for 3 seeds).
- **Simulation duration**: Total time duration of the simulation.
- **File path name**: The name and path for the output file.

# Hash collision statistics
For run tests about the hash collision statistics you can run the following command ([HashCollisionStatistics](algorithm/src/main/scala/it/unibo/big/streamprofiling/algorithm/app/HashCollisionStatistics.scala)).

If needed build the image with the following command:
```bash
docker build -t stream-profiling -f Dockerfile .
```

Then run:
```bash
docker run -v /${PWD}/stream-profiling:/app/stream-profiling stream-profiling java -cp algorithm/build/libs/algorithm-0.1-all.jar it.unibo.big.streamprofiling.algorithm.app.HashCollisionStatistics
```