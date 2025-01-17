package it.unibo.big.streamprofiling.algorithm.implementation.incremental.twophases

object Debug2PhasesAlgorithmsUtils {

  import TwoPhasesClusteringUtils.{calculateZScores, clusterRadius, getScatteringAndSeparationZScores, obtainDistancesFromClusterElements}
  import it.unibo.big.streamprofiling.algorithm.implementation.incremental.groupfeature.GroupedGroupFeature
  import it.unibo.big.streamprofiling.algorithm.utils.Settings.CONFIG
  import it.unibo.big.utils.FileWriter
  import it.unibo.big.utils.input.SchemaModeling.{ClusteringResult, Schema, Window}

  sealed trait TwoPhasesClusteringOperationType
  case object SPLIT_OP extends TwoPhasesClusteringOperationType
  case object MERGE_OP extends TwoPhasesClusteringOperationType
  case object CLUSTERING extends TwoPhasesClusteringOperationType
  case object CLUSTERING_FROM_K extends TwoPhasesClusteringOperationType

  case object REDUCE_CORESET extends TwoPhasesClusteringOperationType
  case class OperationDebugInfo(operation: TwoPhasesClusteringOperationType, numberOfElements: Int, elapsedTime: Double)

  /**
   * Append a file "2phases.clusters"
   *
   * @param t time of computation
   * @param window window of computation
   * @param clustering a clustering result
   */
  def writeClustersStatistics(t: Long, window: Window, clustering: Map[GroupedGroupFeature, Seq[(GroupedGroupFeature, Double)]], clusteringName: String): Unit = {
    val data = clustering.zipWithIndex.map {
      case ((x, xs), i) =>
        val distances = obtainDistancesFromClusterElements(xs)
        val avgDistance = distances.sum / distances.size
        val avgSSE = distances.map(i => i * i).sum
        val radius: Double = clusterRadius(xs)
        val c = x.centroidSchema
        val schemas = xs.flatMap(_._1.schemas)

        val seed = getSeed(schemas)

        Seq(t, window.start.getTime, window.end.getTime, i, c.value, seed.map(_._1).getOrElse(""), seed.map(_._2.size).getOrElse(""), avgSSE, avgDistance, radius, schemas.size, clusteringName)
    }.toSeq

    writeClustersStatistics(data)
  }

  /**
   *
   * @param operationsTimes operation times of clustering
   * @param clusteringName clustering name
   */
  def writeOperationTimes(operationsTimes: Seq[OperationDebugInfo], clusteringName: String): Unit = {
    val fileName = s"${CONFIG.getString("2phases.operations")}"
    FileWriter.writeFileWithHeader(operationsTimes.map(op => Seq(op.operation, op.numberOfElements, op.elapsedTime, clusteringName)),
      Seq("operation", "numberOfElements", "elapsedTime", "clusteringName"), fileName)
  }


  /**
   * Append a file "2phases.clusters"
   *
   * @param t          time of computation
   * @param window     window of computation
   * @param clustering a clustering result
   */
  def writeClustersStatisticsForClusteringResult(t: Long, window: Window, clustering: ClusteringResult, clusteringName: String): Unit = {
    val data = clustering.zipWithIndex.map {
      case ((x, xs), i) =>
        val avgDistance = xs.map(_._2).sum / xs.size
        val avgSSE = xs.map(x => x._2 * x._2).sum / xs.size
        val schemas = xs.map(_._1)

        val seed = getSeed(schemas)

        Seq(t, window.start.getTime, window.end.getTime, i, x.value, seed.map(_._1).getOrElse(""), seed.map(_._2.size).getOrElse(""), avgSSE, avgDistance, if(xs.nonEmpty) xs.map(_._2).max else null, schemas.size, clusteringName)
    }.toSeq

    writeClustersStatistics(data)

    // write also z-scores
    val scattering = clustering.map {
      case (c, vx) => c -> vx.map(_._2).sum / vx.size
    }

    def clusteringSeparationZScore(clustering: ClusteringResult, scattering: Map[Schema, Double]): (
      Map[(Schema, Schema), Double],
        Map[(Schema, Schema), Double],
        Map[(Schema, Schema), Double],
        Map[(Schema, Schema), Double]) = {
      val clustersWithIndex = clustering.keys.zipWithIndex.map { case (c, i) => (i, c) }.toMap
      val separation = clustersWithIndex.flatMap {
        case (i, centroidX) => clustersWithIndex.filterKeys(_ > i).flatMap {
          case (_, centroidY) =>
            val distance = centroidX.jaccard(centroidY)
            Map(((centroidX, centroidY), distance), ((centroidY, centroidX), distance))
        }
      }
      getScatteringAndSeparationZScores(separation, scattering)
    }
    val (separationZScores, separation, overlappingZScores, overlapping) = clusteringSeparationZScore(clustering, scattering)
    def getValue: Schema => String = (s : Schema) => s.value
    writeDebugDSCScatteringzScore(clusteringName, t, window, calculateZScores(scattering), scattering, getValue)
    writeDebugDSCSeparationZScore(clusteringName, t, window, separationZScores, separation, overlappingZScores, overlapping, getValue)
  }

  /**
   * Append a file "2phases.clusters"
   *
   * @param data the data to write
   */
  private def writeClustersStatistics(data: Seq[Seq[Any]]): Unit = {
    val fileName = s"${CONFIG.getString("2phases.clusters")}"
    FileWriter.writeFileWithHeader(data,
      Seq("t", "window_start", "window_end", "clusterIndex", "centroid", "moreFrequentSeed", "numberOfElementsWithSeed",
        "AVG(sse)", "AVG(dist)", "radius", "numberOfValues", "clusteringName"), fileName)
  }

  /**
   * @param schemas the schemas
   * @return the most frequent seed
   */
  private def getSeed(schemas: Seq[Schema]) = {
    scala.util.Try(schemas.map(s => s.seedName -> 1).groupBy(_._1).maxBy(_._2.size)).toOption
  }

  /**
   * Append a file "2phases.clustersDistances"
   *
   * @param t          time of computation
   * @param window     window of computation
   * @param clustering a clustering result
   */
  def writeClustersDistances(t: Long, window: Window, clustering: Map[GroupedGroupFeature, Seq[(GroupedGroupFeature, Double)]], clusteringName: String): Unit = {
    val clustersWithIndex = clustering.zipWithIndex

    /**
     * @param list a map of elements
     * @param versus the centroid to compare
     * @return the distance from the original centroid, the original from the versus centroid
     */
    def getMinElement(list: Seq[(GroupedGroupFeature, Double)], versus: GroupedGroupFeature): (Double, Double) = {
      if(list.nonEmpty) {
        val (_, d, dVersus) = list.map {
          case (g, d) => (g, d / g.N, g.centroidSchema.jaccard(versus.centroidSchema))
        }.minBy(_._3)
        (d, dVersus)
      } else (0D, 0D)
    }

    val data = clustersWithIndex.flatMap {
      case ((x, xs), xIndex) => clustersWithIndex.collect{
        case ((y, ys), yIndex) if yIndex != xIndex =>
          val (distanceX, distanceMinY) = getMinElement(xs, y)
          val (distanceY, distanceMinX) = getMinElement(ys, x)

          Seq(t, window.start.getTime, window.end.getTime, xIndex, yIndex, x.centroidSchema.value, y.centroidSchema.value, x.centroidSchema.jaccard(y.centroidSchema), clusteringName,
            distanceX, distanceMinY, distanceY, distanceMinX)

      }
    }.toSeq

    val fileName = s"${CONFIG.getString("2phases.clustersDistances")}"
    FileWriter.writeFileWithHeader(data,
      Seq("t", "window_start", "window_end", "clusterIndexX", "clusterIndexY", "centroidX", "centroidY", "distance", "clusteringName", "distanceX", "distanceMinY", "distanceY", "distanceMinX"), fileName)
  }

  /**
   * Writes file to debug min hash operations.
   * @param data the data of min hash operations
   */
  def writeDebugDSC(data: Seq[Seq[Any]]): Unit = {
    val fileName = s"${CONFIG.getString("2phases.operationsDebug")}"
    FileWriter.writeFileWithHeader(data,
      Seq("algorithm", "t", "windowStart", "windowEnd", "operation", "c1", "c2", "zScore", "RadiiVsDistances", "numberOfNewClusters", "opSuccess"), fileName)
  }

  /**
   * Append a file "2phases.zScoreScattering"
   *
   * @param clusteringName name of clustering algorithm
   * @param t time
   * @param window actual window
   * @param scatteringZscores map where for each cluster are scattering zscore
   * @param scattering map where for each cluster are scattering
   * @param getValue a function that given the record return its value
   * @tparam T type of clustered values
   */
  def writeDebugDSCScatteringzScore[T](clusteringName: String, t: Long, window: Window, scatteringZscores: Map[T, Double], scattering: Map[T, Double], getValue : T => String): Unit = {
    val fileName = s"${CONFIG.getString("2phases.zScoreScattering")}"
    FileWriter.writeFileWithHeader(scatteringZscores.map {
      case (c, zScore) => Seq(clusteringName, t, window.start.getTime, window.end.getTime, getValue(c), scattering(c), zScore)
    }.toSeq, Seq("algorithm", "t", "windowStart", "windowEnd", "value", "scattering", "zScoreScattering"), fileName)
  }

  /**
   * Append a file "2phases.zScoreSeparation"
   *
   * @param clusteringName name of clustering algorithm
   * @param t time
   * @param window actual window
   * @param separationZScores map where for each cluster couple are zscore of separation
   * @param separation map where for each cluster couple there are distances
   * @param overlappingZScores map where for each pair of clusters there is the zscore sum of scattering divided by separation
   * @param overlapping map where for each pair of clusters there is the sum of scattering divided by separation
   * @param getValue a function that given the record return its value
   * @tparam T type of clustered values
   */
  def writeDebugDSCSeparationZScore[T](clusteringName: String, t: Long, window: Window, separationZScores: Map[(T, T), Double],
                                       separation: Map[(T, T), Double], overlappingZScores: Map[(T, T), Double],
                                       overlapping: Map[(T, T), Double], getValue: T => String): Unit = {
    val fileName = s"${CONFIG.getString("2phases.zScoreSeparation")}"
    FileWriter.writeFileWithHeader(separationZScores.map{
      case ((c1, c2), zScoreSeparation) => Seq(clusteringName, t, window.start.getTime, window.end.getTime, getValue(c1), getValue(c2), zScoreSeparation, separation((c1, c2)), overlappingZScores((c1, c2)), overlapping((c1, c2)))
    }.toSeq, Seq("algorithm", "t", "windowStart", "windowEnd", "value1", "value2", "zScoreSeparation", "separation", "overlappingZScores", "overlapping_sumscattering_dividedby_distance"), fileName)
  }

}
