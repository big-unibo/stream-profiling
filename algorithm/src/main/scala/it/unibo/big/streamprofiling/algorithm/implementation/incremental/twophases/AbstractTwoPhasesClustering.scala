package it.unibo.big.streamprofiling.algorithm.implementation.incremental.twophases

import it.unibo.big.streamprofiling.algorithm.Metrics.EvaluationMetric
import it.unibo.big.streamprofiling.algorithm.implementation.incremental.IncrementalClustering
import it.unibo.big.streamprofiling.algorithm.implementation.incremental.IncrementalClusteringUtils.clustering
import it.unibo.big.streamprofiling.algorithm.implementation.incremental.groupfeature.{BucketWindowGroupFeature, GroupedGroupFeature, SimplifiedGroupFeature}
import it.unibo.big.streamprofiling.algorithm.implementation.incremental.twophases.Debug2PhasesAlgorithmsUtils.{CLUSTERING, OperationDebugInfo}
import it.unibo.big.streamprofiling.algorithm.implementation.incremental.twophases.TwoPhasesClusteringUtils.clusterRadius
import it.unibo.big.streamprofiling.algorithm.implementation.incremental.twophases.TwoPhasesDataTypes.{TwoPhasesClusterAssignments, TwoPhasesClusteringResult, TwoPhasesCorrespondenceCentroids}
import it.unibo.big.streamprofiling.algorithm.implementation.incremental.twophases.reducecoreset.ReduceCoresetTypes.{HASH_LENGTH, ReduceCoresetType, StandardReducing}
import it.unibo.big.streamprofiling.algorithm.implementation.incremental.twophases.reducecoreset.ReduceCorset
import it.unibo.big.streamprofiling.algorithm.implementation.kmeans.KMeansOnSchemas.jaccardSpace
import it.unibo.big.streamprofiling.algorithm.utils.{MonitoringProperty, MonitoringStats}
import it.unibo.big.utils.input.SchemaModeling._

/**
 * Abstract implementation of the clustering explained in "Efficient Data Stream Clustering With Sliding Windows Based on Locality-Sensitive Hashing"
 *
 * Without the implementation of clustering distance and centroid computation, and buckets creation
 * <a>https://ieeexplore.ieee.org/document/8501907</a>
 *
 * @param windowLength               length of the window
 * @param windowPeriod               length of the period
 * @param m                          coreset size
 * @param l                          number of hash functions
 * @param applyReduceBucketHeuristic true if you want to apply the heuristic to reduce bucket numbers, avoiding comparing the distances of all buckets
 * @param randomInitOfClustering     true if random init, false if kmeans++
 * @param metric                     the metric for evaluate the best k, number of cluster
 * @param reduceCoresetType          implementation of reduce coreset algorithm
 * @param elbowSensitivity           the sensitivity for elbow method, needed when the metric used is the SSE
 * @param maximumNumberOfClusters maximumNumberOfCluster in OMRk
 * @tparam T type of element inside the hash value
 */
abstract class AbstractTwoPhasesClustering[T](windowLength: Long, windowPeriod: Long, m: Int, l: Int, applyReduceBucketHeuristic: Boolean,
                                                                 randomInitOfClustering: Boolean, metric: EvaluationMetric, reduceCoresetType: ReduceCoresetType, elbowSensitivity: Option[Double], maximumNumberOfClusters: Option[Int])
                                                                extends IncrementalClustering[GroupedGroupFeature] {

  import it.unibo.big.streamprofiling.algorithm.implementation.kmeans.KMeans.ResultManipulation.groupDataAndComputeCentroids
  import org.slf4j.{Logger, LoggerFactory}
  /*
  import it.unibo.big.utils.FileWriter
  import it.unibo.big.utils.Settings.CONFIG
  */

  import it.unibo.big.streamprofiling.algorithm.implementation.incremental.IncrementalClusteringUtils.clusteringExtractorGroupFeature
  import it.unibo.big.streamprofiling.algorithm.implementation.incremental.groupfeature.GroupFeatureUtils.bucketOrdering
  import it.unibo.big.streamprofiling.algorithm.utils.ClusteringExtractor.ClusteringExtractor

  import scala.collection.immutable.SortedMap

  private[twophases] val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)
  //a map that for each bucket stores the GF divided per pane and the cluster centroid
  private[twophases] var hashTable = SortedMap[IndexedSeq[T], (BucketWindowGroupFeature[T], Option[GroupedGroupFeature])]()
  private[twophases] var clusteringResult = Map[GroupedGroupFeature, Seq[(GroupedGroupFeature, Double)]]()
  private[twophases] var timedSchema: Seq[(Schema, Long)] = Seq()
  private[twophases] var oldRadius: Map[GroupedGroupFeature, Double] = Map()
  private[twophases] var operationDebug: Seq[OperationDebugInfo] = Seq()
  //variables for dsc debug
  private[twophases] var DSCDebugOperations: Seq[Seq[Any]] = Seq()
  private[twophases] var scattering = Map[GroupedGroupFeature, Double]()
  private[twophases] var scatteringZscore = Map[GroupedGroupFeature, Double]()
  private[twophases] var separationZScores = Map[(GroupedGroupFeature, GroupedGroupFeature), Double]()
  private[twophases] var separation = Map[(GroupedGroupFeature, GroupedGroupFeature), Double]()
  private[twophases] var overlappingZScores = Map[(GroupedGroupFeature, GroupedGroupFeature), Double]()
  private[twophases] var overlapping = Map[(GroupedGroupFeature, GroupedGroupFeature), Double]()
  private[twophases] var correspondenceCentroids: TwoPhasesCorrespondenceCentroids = Map()

  private val clusteringExtractor: ClusteringExtractor[GroupedGroupFeature] = implicitly[ClusteringExtractor[GroupedGroupFeature]]

  /**
   * @return the operation made
   */
  def getOperationsTimes: Seq[OperationDebugInfo] = operationDebug

  //statistics variables fo debug
  private [twophases] var monitoredStatistics = MonitoringStats()

  /**
   * Update of the clustering result.
   *
   * @param t             the time of computation
   * @param window        window of computation
   * @param newSchemas    schemas to add to clustering result
   * @param oldestSchemas schemas to delete on clustering result
   */
  override def updateResult(t: Long, window: Window, newSchemas: Seq[SchemaWithTimestamp], oldestSchemas: Seq[SchemaWithTimestamp]): Unit = {
    //init statistics vars
    monitoredStatistics = MonitoringStats()
    operationDebug = Seq()
    //init of all DSC statistics
    DSCDebugOperations = Seq()
    scattering = Map()
    scatteringZscore = Map()
    separation = Map()
    separationZScores = Map()
    overlapping = Map()
    overlappingZScores = Map()

    if (oldestSchemas.isEmpty && clusteringResult.isEmpty && newSchemas.nonEmpty) {
      firstIterationFunction(newSchemas)
    }
    //keep the old clustering result for metric computations
    val oldClusteringResult = clusteringResult
    //set old clusters radius
    oldRadius = clusteringResult.map{
      case (c, values) => c -> clusterRadius(values)
    }
    //map with updates to cluster centroids and new group features
    var clusterAssignments: TwoPhasesClusterAssignments[T] = Map()
    val startUpdateTableTime = System.currentTimeMillis()
    LOGGER.debug("Start update hash table")
    //clear previous data from hash table
    var totalTimeGenerateNewWindow = 0L
    hashTable = hashTable.empty ++ hashTable.groupBy(_._2._2).flatMap {
      case (Some(centroid), bx) =>
        if (bx.exists(x => !x._2._1.isAllContainedBy(window))) {
          clusterAssignments += centroid -> Seq() //update centroids to change
          val newValues = bx.toSeq.collect {
            case (hash, (gf, c)) if gf.existIn(window) =>
              val t = System.currentTimeMillis()
              val res = (hash, (if (gf.isAllContainedBy(window)) gf else gf.generateNewFrom(window), c))
              totalTimeGenerateNewWindow += System.currentTimeMillis() - t
              res
          }
          clusteringResult += centroid -> newValues.map(x => x._2._1 -> 0D)
          newValues
        } else bx
      case _ => Map[IndexedSeq[T], (BucketWindowGroupFeature[T], Option[GroupedGroupFeature])]()
    }
    monitoredStatistics = monitoredStatistics.updateValue(MonitoringProperty.TIME_FOR_REMOVE_OLD_RECORDS, System.currentTimeMillis() - startUpdateTableTime)
    monitoredStatistics = monitoredStatistics.updateValue(MonitoringProperty.TIME_FOR_GENERATE_NEW_WINDOW, totalTimeGenerateNewWindow)
    LOGGER.debug(s"End update hash table takes (${monitoredStatistics.getValue(MonitoringProperty.TIME_FOR_REMOVE_OLD_RECORDS)}) tot. time for generate new window = $totalTimeGenerateNewWindow")

    if (newSchemas.nonEmpty) {
      val newSchemasTimed = newSchemas.map(s => s -> s.timestamp.getTime)
      timedSchema = timedSchema.filter(x => window.contains(x._2)) ++ newSchemasTimed

      if (incrementalUpdateOfResult(newSchemas, window, t)) {
        clusterAssignments = getHashOfSchemas(newSchemasTimed, clusterAssignments)
        val startUpdateResult = System.currentTimeMillis()
        //assign the new data to nearest centroid
        if (clusteringResult.nonEmpty) {
          //for DSC add a new cluster only if there was just a coreset in the previous window
          var firstIteration = true
          for (h <- hashTable.filter(_._2._2.isEmpty).keys) {
            val gf = hashTable(h)._1
            val bestCluster = addElementToCluster(window, gf, firstIteration && clusteringResult.size == 1)
            if (bestCluster.isDefined) {
              clusterAssignments += (bestCluster.get -> (clusterAssignments.getOrElse(bestCluster.get, Seq()) :+ gf))
            }
            firstIteration = false
          }
          correspondenceCentroids = updateClusters(clusterAssignments, oldClusteringResult.keySet)
          monitoredStatistics = monitoredStatistics.updateValue(MonitoringProperty.TIME_FOR_UPDATE_RESULT, System.currentTimeMillis() - startUpdateResult)
          //call method for check if a kind of reclustering is needed
          val clusteringTotalTime = System.currentTimeMillis()
          checkReclustering(oldClusteringResult, correspondenceCentroids, t, window)
          monitoredStatistics = monitoredStatistics.updateValue(MonitoringProperty.TIME_FOR_WHOLE_RECLUSTERING, System.currentTimeMillis() - clusteringTotalTime)
        } else {
          firstClusterGeneration(t, window)
        }
      }
    }
    monitoredStatistics = monitoredStatistics.updateValue(MonitoringProperty.NUMBER_OF_BUCKETS, hashTable.size, replace = true)
  }

  /**
   * @param window the window
   * @param gf a gf to insert in the result
   * @param firstIterationWithOneCluster true for the first time a new bucket is detected considering the previous result was with just a cluster
   * @return the nearest to insert the element in, if present otherwise None
   */
  private [twophases] def addElementToCluster(window: Window, gf: BucketWindowGroupFeature[T], firstIterationWithOneCluster: Boolean): Option[GroupedGroupFeature] = Some(getNearestCluster(window, gf)._1)

  /**
   * Method for check if a kind of reclustering is needed, and if necessary apply it
   *
   * @param oldClusteringResult the previous clustering result
   * @param correspondenceCentroids correspondence map between old clusters and new clusters
   * @param t time of computation
   * @param window window of computation
   */
  private [twophases] def checkReclustering(oldClusteringResult: TwoPhasesClusteringResult, correspondenceCentroids: TwoPhasesCorrespondenceCentroids, t: Long, window: Window): Unit

  /**
   * Get the nearest cluster of a GF and the distance
   *
   * @param window the window
   * @param gf a gf to insert in the result
   * @return the nearest cluster, its radius and the distance
   */
  private [twophases] def getNearestCluster(window: Window, gf: BucketWindowGroupFeature[T]): (GroupedGroupFeature, Double, Double) = {
    val t = System.currentTimeMillis()
    val distances = clusteringResult.map{
      case (c, _) => (c, oldRadius(c), vectorSpace.distance(c, gf) / gf.N)
    }
    monitoredStatistics = monitoredStatistics.updateValue(MonitoringProperty.TIME_FOR_CALCULATE_DISTANCES_BUCKET_CLUSTERS, System.currentTimeMillis() - t)
    distances.minBy(_._3)
  }

  /**
   * Update clusters with assignments and update centroids
   *
   * @param clusterAssignments assignments of newest data to clusters
   * @param oldClusterCenters  centers of old clustering result
   * @return correspondence map between old clusters and new clusters
   */
  private def updateClusters(clusterAssignments: TwoPhasesClusterAssignments[T], oldClusterCenters: Set[GroupedGroupFeature]): TwoPhasesCorrespondenceCentroids = {
    val time = System.currentTimeMillis()
    //recompute changed clusters
    val newClusters = clusterAssignments.map {
      case (centroid, values) =>
        val clusterMembers = clusteringResult(centroid).map(_._1)
        values ++ clusterMembers
    }.toSeq
    clusteringResult = clusteringResult.filterKeys(x => !clusterAssignments.keySet.contains(x))
    var correspondenceCentroids: TwoPhasesCorrespondenceCentroids = clusteringResult.keys.map(c => c -> Some(c)).toMap
    val updatedClusters = groupDataAndComputeCentroids(newClusters, vectorSpace)
    // update hash table centroids
    updatedClusters.foreach {
      case (centroid: GroupedGroupFeature, values) =>
        for (x <- values.map(_._1.asInstanceOf[BucketWindowGroupFeature[T]])) {
          if (x.panes.nonEmpty) {
            if (hashTable(x.bucket)._2.isDefined) {
              // this no longer applies if I then move the elements from one cluster to another!
              correspondenceCentroids += (hashTable(x.bucket)._2.get -> Some(centroid))
            }
            hashTable += ((x.bucket, (x, Some(centroid))))
          }
        }
    }
    correspondenceCentroids ++= oldClusterCenters.filter(!correspondenceCentroids.contains(_)).map(c => c -> None)
    hashTable = hashTable.filter(_._2._1.panes.nonEmpty)
    clusteringResult ++= updatedClusters
    monitoredStatistics = monitoredStatistics.updateValue(MonitoringProperty.TIME_FOR_UPDATE_CLUSTERS, System.currentTimeMillis() - time)
    correspondenceCentroids
  }

  /**
   * Generates the first cluster, from hash table
   *
   * @param t      the time of computation
   * @param window the window
   */
  private[twophases] def firstClusterGeneration(t: Long, window: Window): Unit = {
    LOGGER.info("Start produce a new cluster generation")
    //generate the new clustering result
    LOGGER.info(s"Clustering input records = ${hashTable.size}")
    //apply kmeans
    val time = System.currentTimeMillis()
    val records = hashTable.map { case (_, (x, _)) => x}.toSeq
    val clusteringRecords = if (!randomInitOfClustering) records.sortBy(clusteringExtractor.ordering) else records
    val result = clustering(windowLength, windowPeriod, clusteringRecords, t, window, vectorSpace, randomInitOfClustering, metric, maxNumberOfClusters = maximumNumberOfClusters.getOrElse(m), elbowSensitivity = elbowSensitivity)
    if(result.nonEmpty) {
      clusteringResult = result.get
      val clusteringExecutionTime = System.currentTimeMillis() - time
      monitoredStatistics = monitoredStatistics.updateValue(MonitoringProperty.TIME_FOR_CLUSTERING, clusteringExecutionTime)
      operationDebug :+= OperationDebugInfo(CLUSTERING, clusteringRecords.size, clusteringExecutionTime)
      updateHashTable()
    }
    LOGGER.info("End produce a new cluster generation")
  }

  /**
   * updates hash table with clustering result
   */
  private [twophases] def updateHashTable(): Unit = {
    val time = System.currentTimeMillis()
    hashTable = hashTable.empty ++ clusteringResult.flatMap {
      case (centroid, values) =>
        values.map {
          case (x: BucketWindowGroupFeature[T], _) => (x.bucket, (x, Some(centroid)))
        }
    }
    correspondenceCentroids = Map() //correspondence centroids is empty if clustering is made
    monitoredStatistics = monitoredStatistics.updateValue(MonitoringProperty.TIME_FOR_UPDATE_DATA_STRUCTURE, System.currentTimeMillis() - time)
  }

  /**
   * Insert new schemas in hash table and update existing hash cluster members, if the hash table size >= 2m reduce it
   *
   * @param newSchemas         to hash with timestamp
   * @param clusterAssignments the cluster centroid that have to be updated, i.e. clusters with modified data
   * @return updated cluster assignments
   */
  private[twophases] def getHashOfSchemas(newSchemas: Seq[(Schema, Long)], clusterAssignments: TwoPhasesClusterAssignments[T] = Map()): TwoPhasesClusterAssignments[T] = {
    var time = System.currentTimeMillis()
    var clusterAssignmentsNew = clusterAssignments
    LOGGER.debug("Get new hash")
    var newSchemasHashes: Map[IndexedSeq[T], Map[Long, SimplifiedGroupFeature]] = applyHash(newSchemas)
    LOGGER.debug("End new hash")
    monitoredStatistics = monitoredStatistics.updateValue(MonitoringProperty.NUMBER_OF_INPUT_BUCKETS, newSchemasHashes.size, replace = true)
    monitoredStatistics = monitoredStatistics.updateValue(MonitoringProperty.TIME_FOR_ASSIGN_HASH, System.currentTimeMillis() - time)

    time = System.currentTimeMillis()
    if (hashTable.nonEmpty) {
      //update hash table and clustering assignment adding group features with existing hash values
      LOGGER.debug("Start update hash table (insert)")
      hashTable = hashTable.map {
        case (h, (m, Some(c))) if newSchemasHashes.contains(h) =>
          require(clusteringResult.contains(c))
          clusteringResult += c -> clusteringResult(c).filter(_._1 != m)
          val newSchemasMap = newSchemasHashes(h)
          clusterAssignmentsNew += c -> Seq()

          var newWindowGroupFeature = m
          for (xi <- newSchemasMap.values) {
            newWindowGroupFeature = m.add(xi)
          }
          //update cluster element
          clusteringResult += (c -> (clusteringResult(c) :+ (newWindowGroupFeature -> 0D)))
          (h, (newWindowGroupFeature, Some(c)))
        case (h, (m, c)) => (h, (m, c))
      }
      newSchemasHashes = newSchemasHashes.filterKeys(!hashTable.contains(_))
      LOGGER.debug("End update hash table (insert)")
    }

    hashTable ++= newSchemasHashes.map {
      case (h, m) => (h, (BucketWindowGroupFeature(h, m), None))
    }
    monitoredStatistics = monitoredStatistics.updateValue(MonitoringProperty.TIME_FOR_UPDATE_HASH_TABLE_START, System.currentTimeMillis() - time)
    LOGGER.info(s"Actual buckets = ${hashTable.size}")
    if (hashTable.size >= 2 * m) {
      monitoredStatistics = monitoredStatistics.updateValue(MonitoringProperty.NUMBER_OF_REDUCE_CORESET, 1)
      val time = System.currentTimeMillis()
      val r = reduceCoresetType match {
        case reducing: StandardReducing[T] =>
          ReduceCorset.reduceCoreset(clusteringResult, hashTable, clusterAssignmentsNew, applyReduceBucketHeuristic, m, reducing.distanceFunction)
        case HASH_LENGTH => ReduceCorset.reduceCoresetWithHash(clusteringResult, hashTable, clusterAssignmentsNew, m, l)
      }
      monitoredStatistics = monitoredStatistics.updateValue(MonitoringProperty.TIME_FOR_REDUCE_CORESET, System.currentTimeMillis() - time)
      clusterAssignmentsNew = r._3
      hashTable = r._2
      clusteringResult = r._1
      operationDebug :+= r._4
    }
    clusterAssignmentsNew
  }

  /**
   * @param window the current window
   * @return the actual clustering result
   */
  override def actualClusteringResult(window: Window): ClusteringResult = clusteringResult map {
    case (centroid, values) =>
      val c = centroid.centroidSchema
      c -> values.flatMap(_._1.asInstanceOf[BucketWindowGroupFeature[Int]].schemas).map(s => s -> jaccardSpace.distance(c, s))
  }

  /**
   *
   * @param seq the input schemas of first iteration
   */
  private[twophases] def firstIterationFunction(seq: Seq[Schema]): Unit

  /**
   *
   * @param newSchemas a set of new schemas in the pane
   * @param window     actual window
   * @param t          time of the pane
   * @return true if there is the need to an incremental update of the result, false otherwise.
   *         In case it return false it is supposed the result and the hash table will be changed
   */
  private[twophases] def incrementalUpdateOfResult(newSchemas: Seq[SchemaWithTimestamp], window: Window, t: Long): Boolean

  /**
   *
   * @param newSchemas new timed schemas (the time corresponds to the pane)
   * @return a map that for each bucket have a map of GF in the panes
   */
  private[twophases] def applyHash(newSchemas: Seq[(Schema, Long)]): Map[IndexedSeq[T], Map[Long, SimplifiedGroupFeature]]

  /**
   *
   *  @return monitoring statistics of the algorithm
   */
  override def statistics: MonitoringStats = monitoredStatistics

  /**
   *
   * @return the correspondence centroids between old and new clusters
   */
  def getCorrespondenceCentroids: Map[Map[String, Double], Option[Map[String, Double]]]  = correspondenceCentroids.map(x => x._1.centroid -> x._2.map(_.centroid))
}
