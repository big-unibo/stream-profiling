package it.unibo.big.streamprofiling.algorithm.execution

/**
 * Clustering algorithms trait
 */
object ClusteringAlgorithms {

  import ClusteringAlgorithmsImplementations._
  import it.unibo.big.streamprofiling.algorithm.implementation.incremental.feacstream.FeacStreamAlgorithm
  import it.unibo.big.streamprofiling.algorithm.implementation.incremental.twophases.CSCS.CoreSetBasedClusteringWithSlidingWindows
  import it.unibo.big.streamprofiling.algorithm.implementation.incremental.twophases.DSC.DynamicStreamClustering
  import it.unibo.big.streamprofiling.algorithm.implementation.kmeans.KMeansOnSchemas.{centroidPreAggregateFunction, predictCentroid}

    trait ClusteringAlgorithm {

    /**
     * Compute the clustering algorithm with the given parameters
     *
     * @param windowDuration          the window duration
     * @param slideDuration           the slide duration
     * @param frequency               the time in ms that is between the generation of two records (from producer)
     * @param numberOfSeeds           the number of seeds (from producer)
     * @param readAsAStream           if true read the data as a stream considering it already sorted
     * @param debugResult             if true tells to collect clustering result statistics (default false)
     * @param inputSimulationFileName if present the simulations run in a file
     * @param startTime               if present is the simulation start time, otherwise start from the first instance in the file
     * @param endTime                 if present is the simulation end time, otherwise end with the last instance in the file
     * @param randomInit              if true the clustering is initialized randomly, otherwise use a random fixed seed
     */
    def compute(windowDuration: Long, slideDuration: Long, frequency: Long, numberOfSeeds: Long, readAsAStream: Boolean, debugResult: Boolean = false, inputSimulationFileName: Option[String] = None,
                startTime: Option[Long], endTime: Option[Long], randomInit: Boolean): Unit = {
      this match {
        case x if x.isInstanceOf[IncrementalClusteringAlgorithm] =>
          val algorithm = this match {
            case DSC(numberOfHashFunctions, m, metric, reduceCoreset, localReclustering, newVersion, elbowSensitivity, maximumNumberOfClusters) =>
              new DynamicStreamClustering(this.toString, windowLength = windowDuration, windowPeriod= slideDuration, l = numberOfHashFunctions, m = m, useNewVersion = newVersion,
                randomInitOfClustering = randomInit, metric = metric, reduceCoresetType = reduceCoreset, localReclustering = localReclustering, elbowSensitivity = elbowSensitivity, maximumNumberOfClusters=maximumNumberOfClusters)

            case CSCS(numberOfHashFunctions, m, tolerance, metric, reduceCoreset, elbowSensitivity, maximumNumberOfClusters) =>
              new CoreSetBasedClusteringWithSlidingWindows(name = this.toString, windowLength = windowDuration, windowPeriod= slideDuration, l = numberOfHashFunctions, m = m, w = tolerance,
                applyReduceBucketHeuristic = true, randomInitOfClustering = randomInit, metric = metric, reduceCoresetType = reduceCoreset, elbowSensitivity = elbowSensitivity, maximumNumberOfClusters=maximumNumberOfClusters)

            case FEACS(generateNumberOfPopulations, kmeansIterations, percentatageOfElementsForMutation, startingNumberOfClusters, maximumNumberOfClusters) =>
              //val windowSize = ((windowDuration.toDouble / frequency) * numberOfSeeds).toInt // not true for fadein windows
              val initSize = None //((slideDuration.toDouble / frequency) * numberOfSeeds).toInt //as pane length
              val elementsForMutation = (n: Int) => (percentatageOfElementsForMutation * n).floor.toInt
              val saveRawData = true
              new FeacStreamAlgorithm(initSize, generateNumberOfPopulations, kmeansIterations, elementsForMutation, startingNumberOfClusters, maximumNumberOfClusters, saveRawData, randomInit)
          }

          if (inputSimulationFileName.isDefined) {
            IncrementalClusteringExecution.aggregateStoredWindow(windowDuration, slideDuration, inputSimulationFileName.get, frequency, numberOfSeeds, algorithm, this, readAsAStream = readAsAStream, debugResult = debugResult, startTime = startTime, endTime = endTime)
          }
        case OMRkpp(_, kMax, metric, elbowSensitivity) =>
          if (inputSimulationFileName.isDefined) {
            ClusteringExecution.aggregateKRangeWindow(windowDuration, slideDuration, inputSimulationFileName.get, this,
              (records, k) => predictCentroid(records, k, randomInit = randomInit), metric, frequency, numberOfSeeds, kMax, readAsAStream = readAsAStream, debugResult = debugResult, preAggregateFunction = centroidPreAggregateFunction, startTime = startTime, endTime = endTime, randomInit = randomInit, elbowSensitivity = elbowSensitivity)
          }

      }
    }

    /**
     * Clustering parameters, comma separated
     */
    val parameters: String = ""
  }

  def fromString(s: String): ClusteringAlgorithm = s match {
    case x if OMRkpp.fromString(x).nonEmpty => OMRkpp.fromString(x).get
    case x if FEACS.fromString(x).nonEmpty => FEACS.fromString(x).get
    case x if CSCS.fromString(x).nonEmpty => CSCS.fromString(x).get
    case x if DSC.fromString(x).nonEmpty => DSC.fromString(x).get
    case _ => throw new IllegalArgumentException(s"Unknown clustering algorithm: $s")
  }
}
