package it.unibo.big.streamprofiling.algorithm.implementation.incremental.twophases.DSC

import it.unibo.big.streamprofiling.algorithm.implementation.incremental.twophases.TwoPhasesDataTypes.TwoPhasesClusteringResult

/**
 * Object for LocalReclusteringUtils for DSC and DSC+
 */
private [DSC] object LocalReclusteringUtils {

  import it.unibo.big.streamprofiling.algorithm.Metrics._
  import it.unibo.big.streamprofiling.algorithm.implementation.incremental.IncrementalClusteringUtils.clusteringExtractorGroupFeature
  import it.unibo.big.streamprofiling.algorithm.implementation.incremental.groupfeature.GroupedGroupFeature
  import it.unibo.big.streamprofiling.algorithm.implementation.kmeans.KMeans.ResultManipulation.groupDataAndComputeCentroids
  import it.unibo.big.streamprofiling.algorithm.implementation.kmeans.KMeans.kmeans
  import it.unibo.big.streamprofiling.algorithm.utils.VectorSpace.VectorSpace
  import org.slf4j.{Logger, LoggerFactory}

  private val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  /**
   *
   * @param kMin                      minimum value of k to try (if zero insert data in existing clusters)
   * @param kMax                      maximum value of k
   * @param records                   records to cluster
   * @param globalResult              the actual global result
   * @param matchFromClusteringResult the match map from clustering result for k = 0
   * @param vectorSpace               the used vector space
   * @param randomInitOfClustering    true if you use random init of clustering, false if k-means++
   * @param metric                    used evaluation metric
   * @param elbowSensitivity          the sensitivity for elbow method, needed when the metric used is the SSE
   * @return the new clustering result, the new clusters (using the metric as evaluation)
   */
   def addNewClusterAndValidateTheGlobalSolution(
                                                  kMin: Int, kMax: Int, records: Seq[GroupedGroupFeature], globalResult: TwoPhasesClusteringResult,
                                                  matchFromClusteringResult: Map[GroupedGroupFeature, Seq[GroupedGroupFeature]],
                                                  vectorSpace: VectorSpace[GroupedGroupFeature], randomInitOfClustering: Boolean, metric: Metric, elbowSensitivity: Option[Double]
                                                       ): Option[(TwoPhasesClusteringResult, TwoPhasesClusteringResult)] = {
    var bestK = DEFAULT_INDEX
    val result = (kMin to kMax).par.collect { //not use external variable since parallel computation is used
      case k if k <= records.size =>
        var newClusteringResult: TwoPhasesClusteringResult = Map()
        var newClusters: TwoPhasesClusteringResult = Map()
        if (k == 0) {
          require(matchFromClusteringResult.nonEmpty)
          newClusters = groupDataAndComputeCentroids(matchFromClusteringResult.map {
            case (f, xs) => xs ++ globalResult(f).map(_._1)
          }.toSeq, vectorSpace)
          newClusteringResult = globalResult.filterKeys(!matchFromClusteringResult.contains(_)) ++ newClusters
        } else {
          newClusters = (if (k == 1) {
            groupDataAndComputeCentroids(Seq(records), vectorSpace)
          } else {
            kmeans(records, k, vectorSpace, randomInitOfClustering, toCenter = clusteringExtractorGroupFeature.toCenter)
          }).filter(_._2.nonEmpty)
          newClusteringResult = globalResult ++ newClusters
        }
        val metricValue = metric.compute(newClusteringResult, vectorSpace)
        val otherMetricValue = if(metric == SSE) Some(Silhouette.compute(newClusteringResult, vectorSpace)) else None
        LOGGER.debug(s"clusters = ${newClusteringResult.size} (k =$k) - $metric: $metricValue")
        k -> (newClusteringResult, newClusters, metricValue, otherMetricValue)
    }.seq.toMap

    if (metric == SSE && result.size > 1) {
      LOGGER.info("Start compute elbow method")
      bestK = SSE.chooseBestResult(result.map(x => x._1 -> (x._2._1 -> x._2._3)), elbowSensitivity.getOrElse(1D))
      if (bestK == DEFAULT_INDEX) {
        LOGGER.info("No elbow point --- choose k = 2")//LOGGER.info("No elbow point --- Start choose max silhouette value")
        bestK = 2//or use Silhouette.chooseBestResult(result.map(x => x._1 -> (x._2._1 -> x._2._4.get)))
      }
      LOGGER.info(s"End compute elbow method (k = $bestK)")
    } else if (metric == Silhouette && result.size > 1) {
      //Choose max silhouette value, for another check
      LOGGER.info("Start choose max silhouette value")
      bestK = Silhouette.chooseBestResult(result.map(x => x._1 -> (x._2._1 -> x._2._3)))
      LOGGER.info("End compute max silhouette value")
    } else if(result.size == 1) {
      bestK = result.head._1
    }
    if(bestK == DEFAULT_INDEX) None else Some((result(bestK)._1, result(bestK)._2))
  }
}
