package it.unibo.big.streamprofiling.algorithm.implementation.incremental

import it.unibo.big.streamprofiling.algorithm.implementation.kmeans.{KMeans, KMeansWithoutK}

/**
 * Utility object for apply kmeans on a result of clustering, e.g. with a result with an incremental clustering algorithm
 */
private [incremental] object KMeansOnIncrementalClustering {

  import KMeans.kmeans
  import KMeansWithoutK.computeClusteringInARange
  import it.unibo.big.streamprofiling.algorithm.Metrics._
  import it.unibo.big.streamprofiling.algorithm.execution.ClusteringAlgorithmsImplementations.Clustering
  import it.unibo.big.streamprofiling.algorithm.utils.ClusteringExtractor._
  import it.unibo.big.streamprofiling.algorithm.utils.VectorSpace.VectorSpace
  import it.unibo.big.utils.input.SchemaModeling.Window

  /**
   * @tparam T                     the type of clustered data
   * @param windowLength           window length
   * @param windowPeriod           window period
   * @param t                      the time of computation
   * @param window                 window of computation
   * @param records                input records
   * @param kMin                   k min range to try
   * @param kMax                   k max range to try (max number of micro-clusters)
   * @param vectorSpace            the used vector space for compute (if needed) the re_clustering
   * @param randomInitOfClustering true if random init, false if kmeans++
   * @param metric                 the used metric for compute k-range clustering
   * @param toCenter               the function to obtain the data
   * @param elbowSensitivity     the sensitivity for elbow method, needed when the metric used is the SSE
   * @return the best k result
   */
  def clusterDataInRange[T: ClusteringExtractor](windowLength: Long, windowPeriod: Long, t: Long, window: Window, records: Seq[T], kMin: Int, kMax: Int, vectorSpace: VectorSpace[T],
                         randomInitOfClustering: Boolean, metric: EvaluationMetric, toCenter: T => T, elbowSensitivity: Option[Double]): Option[Map[T, Seq[(T, Double)]]] = {
    val (seeds, frequency) = estimateNumberOfSeedsAndFrequency(window, records)
    val (results, bestK) = computeClusteringInARange[T](windowLength, windowPeriod, seeds, frequency, kMin to kMax, t, window, records, Clustering("LSH-reclustering"),
      (xs: Seq[T], k) => kmeans(xs, k, vectorSpace, randomInitOfClustering, toCenter = toCenter), metric, vectorSpace = vectorSpace, elbowSensitivity = elbowSensitivity)
    results.get(bestK).map(_._1)
  }

  /**
   * @param window the actual window
   * @param records records in the window
   * @return the estimation on data frequency and seeds in the window (used only for statistics purposes)
   */
  private def estimateNumberOfSeedsAndFrequency[T : ClusteringExtractor](window: Window, records: Seq[T]): (Long, Long) = {
    //val clusteringExtractor = implicitly[ClusteringExtractor[T]]
    val seeds = 10// is fixed records.map(clusteringExtractor.className).distinct.size
    val windowLen = window.end.getTime - window.start.getTime
    val frequency = ((records.size / windowLen) * 1000) / seeds
    // check but not used as debug result
    (seeds.toLong, frequency)
  }
}
