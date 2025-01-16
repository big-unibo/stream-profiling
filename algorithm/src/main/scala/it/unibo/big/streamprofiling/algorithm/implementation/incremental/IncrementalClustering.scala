package it.unibo.big.streamprofiling.algorithm.implementation.incremental

import it.unibo.big.streamprofiling.algorithm.implementation.incremental.groupfeature.{BucketWindowGroupFeature, GroupFeature, GroupedGroupFeature, RawGroupedGroupFeature}
import it.unibo.big.streamprofiling.algorithm.utils.VectorSpace.VectorSpace
import it.unibo.big.streamprofiling.algorithm.utils.{MonitoringProperty, MonitoringStats}
import it.unibo.big.utils.input.SchemaModeling._

/**
 * Trait for incremental clustering.
 *
 * @tparam T the used type for the vector space
 */
trait IncrementalClustering[T] {
  /**
   * Update of the clustering result.
   *
   * @param t             the time of computation
   * @param window        window of computation
   * @param newSchemas    schemas to add to clustering result
   * @param oldestSchemas schemas to delete on clustering result
   */
  def updateResult(t: Long, window: Window, newSchemas: Seq[SchemaWithTimestamp], oldestSchemas: Seq[SchemaWithTimestamp]): Unit

  /**
   *
   * @return the actual clustering result
   */
  def actualClusteringResult(window: Window): ClusteringResult

  /**
   * @return the used vector space
   */
  def vectorSpace: VectorSpace[T]

  /**
   *
   * @return monitoring statistics of the algorithm
   */
  def statistics: MonitoringStats
}

/**
 * Utils object for incremental clustering
 */
private[algorithm] object IncrementalClusteringUtils {
  import it.unibo.big.streamprofiling.algorithm.Metrics.EvaluationMetric
  import it.unibo.big.streamprofiling.algorithm.utils.ClusteringExtractor._
  import it.unibo.big.streamprofiling.algorithm.utils.VectorSpace.VectorSpace
  import org.slf4j.{Logger, LoggerFactory}

  private val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)
  private[algorithm] val DIVERGENCE_THRESHOLD = 0.2D //epsilon of LSH paper

  /**
   * @tparam T the type of clustered data
   * @param windowLength window length
   * @param windowPeriod window period
   * @param clusteringResult input clustering result
   * @param t the actual time
   * @param window the referred window
   * @param vectorSpace the used vector space for compute (if needed) the re_clustering
   * @param randomInitOfClustering true if random init, false if kmeans++
   * @param metric the used metric for compute k-range clustering and choose best k
   * @param maxNumberOfClusters    max number of clusters (i.e. m of clustering)
   * @param elbowSensitivity   the sensitivity of elbow method
   * @return a new clustering result if reclustering policy tells to made reclustering it applies kmeans with elbow method from 2 to sqrt of number of records
   */
  def reclustering[T: ClusteringExtractor](windowLength: Long, windowPeriod: Long, clusteringResult: Map[T, Seq[(T, Double)]], t: Long, window: Window, vectorSpace: VectorSpace[T],
                      randomInitOfClustering: Boolean, metric: EvaluationMetric, maxNumberOfClusters: Int, elbowSensitivity: Option[Double]): Map[T, Seq[(T, Double)]] = {
    clustering(windowLength, windowPeriod, clusteringResult.flatMap(_._2.map(_._1)).toSeq, t, window, vectorSpace, randomInitOfClustering, metric, maxNumberOfClusters, elbowSensitivity = elbowSensitivity).getOrElse(clusteringResult)
  }

  /**
   * @tparam T the type of clustered data
   * @param windowLength           window length
   * @param windowPeriod           window period
   * @param inputRecords           input clustering records
   * @param t                      the actual time
   * @param window                 the referred window
   * @param vectorSpace            the used vector space for compute (if needed) the re_clustering
   * @param randomInitOfClustering true if random init, false if kmeans++
   * @param metric                 the used metric for compute k-range clustering and choose best k
   * @param maxNumberOfClusters    max number of clusters (i.e. m of clustering)
   * @param elbowSensitivity       the sensitivity of elbow method
   * @return a new clustering result if reclustering policy tells to made reclustering it applies kmeans with elbow method from 2 to sqrt of number of records,
   *         otherwise the actual clustering result
   */
  def clustering[T: ClusteringExtractor](windowLength: Long, windowPeriod: Long, inputRecords: Seq[T], t: Long, window: Window, vectorSpace: VectorSpace[T],
                                           randomInitOfClustering: Boolean, metric: EvaluationMetric, maxNumberOfClusters: Int, elbowSensitivity: Option[Double]): Option[Map[T, Seq[(T, Double)]]] = {
    val clusteringExtractor: ClusteringExtractor[T] = implicitly[ClusteringExtractor[T]]
    var records = inputRecords
    val maxClusters: Int = getMaximumNumberOfClusters(maxNumberOfClusters, records)
    if (!randomInitOfClustering) {
      records = records.sortBy(clusteringExtractor.ordering)
    }
    KMeansOnIncrementalClustering.clusterDataInRange(windowLength, windowPeriod, t, window, records, 2, maxClusters, vectorSpace, randomInitOfClustering, metric, clusteringExtractor.toCenter, elbowSensitivity = elbowSensitivity)
  }

  /**
   *
   * @param upperLimit maximum number of cluster, for algorithm. E.g., m
   * @param records seq of records
   * @tparam T type of records
   * @return the number of clusters: is min(sqrt(numberOfRecords), maxNumberOfClusters)) where
   *         numberOfRecords is sum of gf.N, in case of group features
   */
  def getMaximumNumberOfClusters[T: ClusteringExtractor](upperLimit: Int, records: Seq[T]): Int = {
    val recordsNumber : Int = records.map {
      case x: GroupFeature => x.N
      case _ => 1
    }.sum
    Math.min(math.ceil(math.sqrt(recordsNumber)).toInt, upperLimit)
  }

  /**
   * @tparam T the type of clustered data
   * @param windowLength window length
   * @param windowPeriod window period
   * @param actualClusteringResult the actual clustering result
   * @param oldClusteringResult    the oldest clustering result
   * @param correspondenceCentroids the centroid correspond from old clustering result to the newest one
   * @param t                      the time of computation
   * @param window                 window of computation
   * @param vectorSpace            the used vector space for compute (if needed) the re_clustering
   * @param randomInitOfClustering true if random init, false if kmeans++
   * @param metric                 the used metric for compute k-range clustering and choose best k
   * @param clustersKLDivergence   a computed map of kl divergence cluster by cluster, default empty and if not empty is used for not recompute kl divergence
   * @param maxNumberOfClusters    max number of clusters (i.e. m of clustering)
   * @param elbowSensitivity       the sensitivity of elbow method
   * @param statistics             the monitoring statistics
   * @return the actual clustering result, re-clustered if the divergence is above the threshold and a boolean flag that tells if reclustering has been made and the new monitoring statistics
   */
  def reclusteringIfNeeded[T:ClusteringExtractor](windowLength: Long, windowPeriod: Long, actualClusteringResult: Map[T, Seq[(T, Double)]], oldClusteringResult: Map[T, Seq[(T, Double)]],
                                                  correspondenceCentroids: Map[T, Option[T]], t: Long, window: Window,
                                                  vectorSpace: VectorSpace[T], randomInitOfClustering: Boolean, metric: EvaluationMetric, clustersKLDivergence: Map[T, Double] = Map[T, Double](),
                                                  elbowSensitivity: Option[Double], maxNumberOfClusters: Int, statistics : MonitoringStats): (Map[T, Seq[(T, Double)]], Boolean, MonitoringStats) = {
    if(oldClusteringResult.isEmpty) {
      (actualClusteringResult, false, statistics)
    } else {
      var newStatistics = statistics
      var time = System.currentTimeMillis()
      val klDivergence = if(clustersKLDivergence.isEmpty) {
        kullbackLeiblerDivergence(actualClusteringResult, oldClusteringResult, correspondenceCentroids, vectorSpace)
      } else aggregateKLDivergence(clustersKLDivergence)
      newStatistics = newStatistics.updateValue(MonitoringProperty.TIME_FOR_TEST_RESULT, System.currentTimeMillis() - time)


      if(klDivergence > DIVERGENCE_THRESHOLD) {
        time = System.currentTimeMillis()
        val result = reclustering(windowLength, windowPeriod, actualClusteringResult, t, window, vectorSpace, randomInitOfClustering, metric, maxNumberOfClusters, elbowSensitivity = elbowSensitivity)
        newStatistics = newStatistics.updateValue(MonitoringProperty.TIME_FOR_CLUSTERING, System.currentTimeMillis() - time)
        newStatistics = newStatistics.updateValue(MonitoringProperty.NUMBER_OF_RECLUSTERING, 1)
        (result, true, newStatistics)
      } else (actualClusteringResult, false, newStatistics)
    }
  }

  /**
   * @tparam T the type of clustered data
   * @param newClusteringResult newest clustering result
   * @param oldClusteringResult oldest clustering result
   * @param correspondenceCentroids the centroid correspond from old clustering result to the newest one
   * @param vectorSpace the used vector space
   * @return Kullback-Leibler divergence from the actual clustering result and the oldest one
   */
  private def kullbackLeiblerDivergence[T](newClusteringResult: Map[T, Seq[(T, Double)]], oldClusteringResult: Map[T, Seq[(T, Double)]], correspondenceCentroids: Map[T, Option[T]], vectorSpace: VectorSpace[T]): Double = {
    aggregateKLDivergence(clusterKullbackLeiblerDivergence(newClusteringResult, oldClusteringResult, correspondenceCentroids, vectorSpace))
  }

  /**
   *
   * @param partialResult of KL divergence cluster by cluster
   * @tparam T type of clustered data
   * @return the overall KL as a mean
   */
  private def aggregateKLDivergence[T](partialResult: Map[T, Double]): Double = {
    val result = partialResult.values.sum / partialResult.size //average of KL for each centroid
    LOGGER.info (s"Kullback Leibler divergence is $result (threshold is $DIVERGENCE_THRESHOLD)")
    result
  }

  /**
   * @tparam T the type of clustered data
   * @param newClusteringResult     newest clustering result
   * @param oldClusteringResult     oldest clustering result
   * @param correspondenceCentroids the centroid correspond from old clustering result to the newest one
   * @param vectorSpace             the used vector space
   * @return Kullback-Leibler divergence for each cluster of the actual clustering result and the oldest one
   */
  private def clusterKullbackLeiblerDivergence[T](newClusteringResult: Map[T, Seq[(T, Double)]], oldClusteringResult: Map[T, Seq[(T, Double)]], correspondenceCentroids: Map[T, Option[T]], vectorSpace: VectorSpace[T]): Map[T, Double] = {
    var pq = Map[T, (IndexedSeq[Double], IndexedSeq[Double])]() //collect p and q for each centroid

    def getProbability(seq: IndexedSeq[(T, Double)], f: (T, Double) => Double): IndexedSeq[Double] = {
      val tmp = seq.map(x => f(x._1, x._2))
      val sum = tmp.sum
      tmp.map {
        case x if sum > 0 => x / sum
        case _ => 0 //prevent NaN results
      }
    }

    for ((oldCenter, oldElements) <- oldClusteringResult) {
      if (correspondenceCentroids(oldCenter).isDefined) {
        val newCenter = correspondenceCentroids(oldCenter).get
        val newElements = newClusteringResult(newCenter).toIndexedSeq
        //distance from old center
        val p = getProbability(newElements, (x, _) => Math.pow(oldElements.filter(_._1 == x).map(_._2).headOption.getOrElse(vectorSpace.distance(oldCenter, x)), 2))
        //distance from newCenter
        val q = getProbability(newElements, (_, d) => Math.pow(d, 2))
        pq += (newCenter -> (p -> q))
      }
    }
    pq.map {
      case (c, (p, q)) => c -> p.indices.collect { //prevent NaN results
        case x if p(x) != 0 && q(x) != 0 => p(x) * Math.log(p(x) / q(x))
      }.sum
    }
  }

  /**
   * @return the clustering extractor for GroupedGroupFeature
   */
  implicit val clusteringExtractorGroupFeature: ClusteringExtractor[GroupedGroupFeature] = new ClusteringExtractor[GroupedGroupFeature] {
    override def ordering(x: GroupedGroupFeature): String = x.centroidSchema.value

    override def className(x: GroupedGroupFeature): String = x.seedName

    override def toCenter(x: GroupedGroupFeature): GroupedGroupFeature = x match {
      case x: BucketWindowGroupFeature[Any] => new RawGroupedGroupFeature(x.t, x.N, x.LS, x.SS)
      case x => x
    }
  }
}
