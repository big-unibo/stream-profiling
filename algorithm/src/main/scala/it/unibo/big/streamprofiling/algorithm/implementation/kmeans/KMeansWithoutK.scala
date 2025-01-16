package it.unibo.big.streamprofiling.algorithm.implementation.kmeans

object KMeansWithoutK {

  import KMeans.ResultManipulation
  import it.unibo.big.streamprofiling.algorithm.Metrics._
  import it.unibo.big.streamprofiling.algorithm.execution.ClusteringAlgorithms.ClusteringAlgorithm
  import it.unibo.big.streamprofiling.algorithm.utils.ClusteringExtractor._
  import it.unibo.big.streamprofiling.algorithm.utils.DebugWriter.WindowTimeStatistics
  import it.unibo.big.streamprofiling.algorithm.utils.VectorSpace.VectorSpace
  import it.unibo.big.utils.input.SchemaModeling._
  import org.slf4j.{Logger, LoggerFactory}

  private val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  /**
   * @tparam T                         the type fo clustered data
   * @param windowLength               window length
   * @param windowPeriod               window period
   * @param numberOfSeed               number of seeds, clusters
   * @param dataFrequency              data frequency
   * @param kRange                     sorted range of ks values, if present use them otherwise the pre-aggregate function result
   * @param t                          the computation time
   * @param window                     the computation window
   * @param records                    input records
   * @param clustering            clustering method used
   * @param aggregateFunction          aggregate function to compute clustering, can use a precomputed value
   * @param bestKMetric                metric to choose the best k
   * @param preAggregateResult a pre-aggregate result (for hierarchical clustering), for each k generates a result
   * @param vectorSpace vector space for compute the calculation of statistic in a different vector space
   * @param elbowSensitivity           the sensitivity for elbow method, needed when the metric used is the SSE
   * @return all the results for each k and the actual best k
   */
  def computeClusteringInARange[T:ClusteringExtractor](windowLength: Long, windowPeriod: Long, numberOfSeed: Long, dataFrequency: Long, kRange: Seq[Int], t: Long, window: Window, records: Seq[T], clustering: ClusteringAlgorithm,
                                                       aggregateFunction: (Seq[T], Int) => Map[T, Seq[(T, Double)]], bestKMetric: EvaluationMetric,
                                                       preAggregateResult: Map[Int, Seq[Seq[T]]] = Map[Int, Seq[Seq[T]]](), vectorSpace: VectorSpace[T], elbowSensitivity: Option[Double]): (Map[Int, (Map[T, Seq[(T, Double)]], WindowTimeStatistics[Int, T])], Int) = {
    var timeStatistics = Seq[WindowTimeStatistics[Int, T]]()

    //a boolean variable that tells to compute the k variation of the algorithm
    //var compute = true
    var bestK = DEFAULT_INDEX

    //var lastMetricValue: Option[(Double, Int)] = None
    val iterValues = if(kRange.isEmpty) preAggregateResult.filterKeys(x => x >= 2 && x <= math.floor(math.sqrt(records.size)).toInt) //Reduce number of k
      .toSeq.sortBy(_._1) else kRange.map(x => x -> Seq())
    val results = iterValues.par.collect {
      case (k, r) if /*compute &&*/ k <= records.size =>
        val startComputationTime = System.currentTimeMillis()
        LOGGER.debug(s"Start compute window with $clustering k = $k")
        val result = if(r.isEmpty) aggregateFunction(records, k) else ResultManipulation.groupDataAndComputeCentroids(r, vectorSpace)
        val time = System.currentTimeMillis() - startComputationTime
        //pass selected metrics
        val timeStat: WindowTimeStatistics[Int, T] = WindowTimeStatistics(t, window, k, clustering, records.size, time, result, if(bestKMetric == SSE) Seq(bestKMetric, Silhouette) else Seq(bestKMetric), numberOfSeed, dataFrequency,
          vectorSpace, windowLength, windowPeriod)
        timeStatistics :+= timeStat
        LOGGER.debug(s"$clustering: (time $time) window $window ${timeStat.metricsValues.mkString(", ")} - clusters = ${result.size} (k =$k)")
        /*if (bestKMetric == Silhouette) {
          //Silhouette stop condition
          val bestComputation = Silhouette.chooseBestResult(timeStat.metricsValues(bestKMetric), lastMetricValue, k, records.size)
          bestK = bestComputation._1
          lastMetricValue = Some((bestComputation._2, k))
          compute = bestComputation._3
        }*/
        k -> (result, timeStat)
    }.seq.toMap

    // debug for elbow
    val resultSeq = results.map(x => x._1 -> x._2._2.metricsValues(bestKMetric)).toSeq.sortBy(_._1)
    val x = resultSeq.map(_._1)
    val y = resultSeq.map(_._2)
    val xString = "\"" + s"[${x.mkString(",")}]" + "\""
    val yString = "\"" + s"[${y.mkString(",")}]" + "\""
    LOGGER.info("x = " + xString)
    LOGGER.info("y = " + yString)
    if (bestKMetric == SSE && results.size > 1) {
      LOGGER.info("Start compute elbow method")
      bestK = SSE.chooseBestResult(results.map(x => x._1 -> (x._2._1 -> x._2._2.metricsValues(SSE))), elbowSensitivity.getOrElse(1.0))
      if (bestK == DEFAULT_INDEX) {
        LOGGER.info("No elbow point --- Start choose max silhouette value")
        Silhouette.chooseBestResult(results.map(x => x._1 -> (x._2._1 -> x._2._2.metricsValues(Silhouette))))
      }
      LOGGER.info("End compute elbow method")
    } else if (bestKMetric == Silhouette && results.size > 1) {
      //Choose max silhouette value, for another check
      LOGGER.info("Start choose max silhouette value")
      bestK = Silhouette.chooseBestResult(results.map(x => x._1 -> (x._2._1 -> x._2._2.metricsValues(Silhouette))))
      LOGGER.info("End compute max silhouette value")
    }

    if (results.nonEmpty) {
      if (bestK == DEFAULT_INDEX) {
        bestK = results.keys.max //define best k as kmax
      }
    }
    (results, bestK)
  }
}
