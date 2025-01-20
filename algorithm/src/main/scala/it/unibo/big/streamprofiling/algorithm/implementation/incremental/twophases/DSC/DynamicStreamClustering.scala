package it.unibo.big.streamprofiling.algorithm.implementation.incremental.twophases.DSC

import it.unibo.big.streamprofiling.algorithm.Metrics._
import it.unibo.big.streamprofiling.algorithm.execution.ClusteringAlgorithmsImplementations.LocalReclustering
import it.unibo.big.streamprofiling.algorithm.implementation.incremental.IncrementalClusteringUtils.getMaximumNumberOfClusters
import it.unibo.big.streamprofiling.algorithm.implementation.incremental.twophases.CSCS.CoreSetBasedClusteringWithSlidingWindows
import it.unibo.big.streamprofiling.algorithm.implementation.incremental.twophases.Debug2PhasesAlgorithmsUtils
import it.unibo.big.streamprofiling.algorithm.implementation.incremental.twophases.reducecoreset.ReduceCoresetTypes.{ReduceCoresetType, STANDARD}
import it.unibo.big.streamprofiling.algorithm.utils.ClusteringExtractor._
import it.unibo.big.streamprofiling.algorithm.utils.MonitoringProperty
import it.unibo.big.streamprofiling.algorithm.utils.RandomUtils.RANDOM_SEED
import it.unibo.big.utils.input.SchemaModeling.SchemaWithTimestamp

import scala.util.Random

/**
 * Implementation of the Dynamic Stream Clustering algorithm
 *
 * @param name                       clustering name representation
 * @param windowLength               length of the window
 * @param windowPeriod               length of the period
 * @param m                          coreset size
 * @param l                          number of hash functions
 * @param metric                     the metric for evaluate the best k, number of cluster
 * @param useNewVersion true if want to use new version (DSC+): apply the heuristic to reduce bucket numbers, avoiding comparing the distances of all buckets (default true) and split without omrk
 * @param randomInitOfClustering     true if random init, false if kmeans++ (default true)
 * @param reduceCoresetType          type of applied to reduce coreset implementation (default STANDARD, i.e. Manhattan)
 * @param localReclustering          optional parameters for local reclustering
 * @param elbowSensitivity          the sensitivity for elbow method, needed when the metric used is the SSE
 * @param maximumNumberOfClusters   maximumNumberOfCluster in OMRk
 */
class DynamicStreamClustering(name: String, windowLength: Long, windowPeriod: Long, m: Int, l: Int, metric: EvaluationMetric, useNewVersion: Boolean = true,
                              randomInitOfClustering: Boolean = false, reduceCoresetType: ReduceCoresetType = STANDARD, localReclustering: Option[LocalReclustering],
                              elbowSensitivity: Option[Double], maximumNumberOfClusters: Option[Int])
  extends CoreSetBasedClusteringWithSlidingWindows(name, windowLength, windowPeriod, m, l, None, metric, applyReduceBucketHeuristic = useNewVersion, randomInitOfClustering, reduceCoresetType, elbowSensitivity, maximumNumberOfClusters) {

  import Debug2PhasesAlgorithmsUtils._
  import it.unibo.big.streamprofiling.algorithm.implementation.incremental.IncrementalClusteringUtils.clusteringExtractorGroupFeature
  import it.unibo.big.streamprofiling.algorithm.implementation.incremental.groupfeature.GroupFeatureVectorSpace.vectorGroupedSpace
  import it.unibo.big.streamprofiling.algorithm.implementation.incremental.groupfeature.{BucketWindowGroupFeature, GroupedGroupFeature, GroupedGroupFeatureAggregator}
  import it.unibo.big.streamprofiling.algorithm.implementation.incremental.twophases.TwoPhasesClusteringUtils._
  import it.unibo.big.streamprofiling.algorithm.implementation.incremental.twophases.TwoPhasesDataTypes._
  import it.unibo.big.streamprofiling.algorithm.implementation.kmeans.KMeans.ResultManipulation.groupDataAndComputeCentroids
  import it.unibo.big.streamprofiling.algorithm.utils.VectorSpace.VectorSpace
  import it.unibo.big.utils.input.SchemaModeling.{Schema, Window}

  import scala.util.hashing.MurmurHash3


  private val numberOfPanes = windowLength / windowPeriod
  private val worstCaseSSDecrese = 1D / numberOfPanes
  override val vectorSpace: VectorSpace[GroupedGroupFeature] = vectorGroupedSpace((s1, s2) => s1.jaccard(s2))
  //for each data not insert mantain the nearest cluster
  private var insertBucket: Map[BucketWindowGroupFeature[Int], GroupedGroupFeature] = Map()
  //number of panes with split / merge consecutive
  private var numberOfConsecutivePanesWithSplitOrMerge = 0

  override def firstIterationFunction(seq: Seq[Schema]): Unit = {
    numberOfConsecutivePanesWithSplitOrMerge = 0
  }

  /**
   *
   * @param s an input schema
   * @return the signature for the schema considering l hash functions
   */
  override def g(s: Schema): IndexedSeq[Int] = (1 to l) map (i => s.values.map(a => applyHash(a._1, i)).min)

  /**
   *
   * @param attribute an attribute name
   * @param seed      the seed for the hash function
   * @return the hash value for the attribute within the seed
   */
  private def applyHash(attribute: String, seed: Int): Int = {
    Random.setSeed(RANDOM_SEED)
    MurmurHash3.stringHash(attribute, seed)
  }

  /**
   * @param newSchemas the new schemas
   * @param window     the actual window
   * @param t          the time of computation
   * @return true if the result can be updated (always true). Differently from CSCS that needs to check if the dictionary is changed.
   */
  override def incrementalUpdateOfResult(newSchemas: Seq[SchemaWithTimestamp], window: Window, t: Long): Boolean = {
    insertBucket = Map() //re-init insert bucket
    true //always can update the result
  }

  /**
   * @param gf a grouped group feature
   * @return the value of the schema that is the centroid
   */
  private def getValue(gf: GroupedGroupFeature): String = gf.centroidSchema.value

  /**
   * Check if reclustering is needed, if so, perform it
   * @param oldClusteringResult the previous clustering result
   * @param correspondenceCentroids the correspondence centroids
   * @param t the time of computation
   * @param window the actual window
   */
  override def checkReclustering(oldClusteringResult: TwoPhasesClusteringResult, correspondenceCentroids: TwoPhasesCorrespondenceCentroids, t: Long, window: Window): Unit = {
    val clusteringExtractor: ClusteringExtractor[GroupedGroupFeature] = implicitly[ClusteringExtractor[GroupedGroupFeature]]
    //remove oldest clusters
    val (nonEmptyClusters, emptyClusters) = clusteringResult.partition(_._2.nonEmpty)
    val maxNumberOfClusters = getMaximumNumberOfClusters(maximumNumberOfClusters.getOrElse(hashTable.size), clusteringResult.flatMap(_._2.map(_._1)).toSeq)
    LOGGER.info(s"Maximum number of clusters is $maxNumberOfClusters, limit is exceeded ${clusteringResult.size > maxNumberOfClusters}")

    if(emptyClusters.nonEmpty) {
      //"algorithm", "t", "windowStart", "windowEnd", "operation", "c1", "c2", "zScore", "RadiiVsDistances", "numberOfNewClusters", "opSuccess"
      DSCDebugOperations ++= emptyClusters.keys.map(c => Seq(name, t, window.start.getTime, window.end.getTime, FADEOUT(c), c.centroidSchema.value, null, null, null, 1, true)).toSeq
      LOGGER.info(s"Fade out ${emptyClusters.map(_._1.centroidSchema.value)}")
    }
    clusteringResult = nonEmptyClusters
    if (localReclustering.isEmpty) {
      // debug z-score values
      scattering = clusteringResult.map { case (c, vx) => c -> clusterScattering(vx) }
      scatteringZscore = calculateZScores(scattering)
      calculateSeparationZScore()
      //super.checkReclustering(oldClusteringResult, correspondenceCentroids, t, window)
    } else {
      //1) recluster data in insertBucket
      if (insertBucket.nonEmpty) {
        clusterInsertBucket(t, window, correspondenceCentroids)
      } else {
        insertBucket = Map()
      }
      //check z-score
      var worstCluster = findWorstCluster(t, window, maxNumberOfClusters, None)
      var compute = true
      var newClusteringResult : TwoPhasesClusteringResult = Map()
      var clusteringResultPrev: TwoPhasesClusteringResult = clusteringResult
      var newClusters : TwoPhasesClusteringResult = Map()
      var debugOperations: Seq[Seq[Any]] = Seq()
      val time = System.currentTimeMillis()
      val oldClusteringResultSilhouette = Silhouette.compute(oldClusteringResult, vectorSpace)
      var reclusteringMade = false //became true after a reclustering or an operation of forced merge with success
      monitoredStatistics = monitoredStatistics.updateValue(MonitoringProperty.TIME_FOR_TEST_RESULT, System.currentTimeMillis() - time)

      if(worstCluster.isEmpty) {
        numberOfConsecutivePanesWithSplitOrMerge = 0
      } else {
        numberOfConsecutivePanesWithSplitOrMerge += 1
      }

      while(worstCluster.nonEmpty && compute) {
        //"algorithm", "t", "windowStart", "windowEnd", "operation", "c1", "c2", "zScore", "RadiiVsDistances", "numberOfNewClusters", "opSuccess"
        var debugSeq = Seq(name, t, window.start.getTime, window.end.getTime, worstCluster.get.toString)

        worstCluster.get match {
          case SPLIT(cluster, zScore) =>
            val time = System.currentTimeMillis()
            LOGGER.debug(s"Split zScore = $zScore ${cluster.centroidSchema.value}")
            val records = clusteringResult(cluster).map(_._1)
            //get max of new clusters for split considering maximum number of clusters
            val maxNumberOfNewClusters = if(useNewVersion) 2 else math.min(records.size, maxNumberOfClusters - clusteringResult.size + 1)
            val result = LocalReclusteringUtils.addNewClusterAndValidateTheGlobalSolution(2, maxNumberOfNewClusters, if (!randomInitOfClustering) records.sortBy(clusteringExtractor.ordering) else records,
              clusteringResult.filterKeys(_ != cluster), Map(), vectorSpace, randomInitOfClustering, metric, elbowSensitivity)
            if(result.nonEmpty) {
              newClusteringResult = result.get._1
              newClusters = result.get._2
              debugSeq ++= Seq(cluster.centroidSchema.value, null, zScore, null, newClusters.size)
              val timeForSplit = System.currentTimeMillis() - time
              operationDebug :+= OperationDebugInfo(Debug2PhasesAlgorithmsUtils.SPLIT_OP, records.size, timeForSplit)
              monitoredStatistics = monitoredStatistics.updateValue(MonitoringProperty.TIME_FOR_SPLIT, timeForSplit)
            } else {
              newClusteringResult = clusteringResultPrev
              newClusters = Map()
              compute = false //stop to get stuck in wrong splits
            }
          case MERGE((c1, c2), zScore, radiiVsDistance, _) =>
            //operationForReduceClustersNumber = mergeForReduceK
            val time = System.currentTimeMillis()
            LOGGER.debug(s"Merge zScore = $zScore and radii/distance = $radiiVsDistance ${c1.centroidSchema.value} and ${c2.centroidSchema.value}")
            //filter out clusters to merge from clustering result
            val clustersToMerge = Seq(c1, c2)
            val (mergeClusters, partialClusteringResult) = clusteringResult.partition(k => clustersToMerge.contains(k._1))
            //create the new cluster
            newClusters = groupDataAndComputeCentroids(Seq(mergeClusters.flatMap(_._2.map(_._1)).toSeq), vectorSpace)
            //update clustering result
            newClusteringResult = partialClusteringResult ++ newClusters
            val timeForMerge = System.currentTimeMillis() - time
            debugSeq ++= Seq(c1.centroidSchema.value, c2.centroidSchema.value, zScore, radiiVsDistance, newClusters.size)
             operationDebug :+= OperationDebugInfo(Debug2PhasesAlgorithmsUtils.MERGE_OP, 2, timeForMerge)
            monitoredStatistics = monitoredStatistics.updateValue(MonitoringProperty.TIME_FOR_MERGE, timeForMerge)
        }

        var proceed = true
        if(!useNewVersion) {
          val time = System.currentTimeMillis()
          val actualMetric = Silhouette.compute(newClusteringResult, vectorSpace)
          /*val proceed = if(operationForReduceClustersNumber) {
            //In case of a merge to reduce k, the comparison is valid only if it doesn't fall below the worst-case threshold
            val proceedSpecialMerge = (oldClusteringResultSilhouette - actualMetric) >= worstCaseSSDecrese
            LOGGER.debug(s"Actual Silhouette without the ${worstCluster.get.toString} (FORCED) was in the window $oldClusteringResultSilhouette, can proceed $proceedSpecialMerge.")
            proceedSpecialMerge
          } else {*/
          val previousClusteringResultMetric = Silhouette.compute(clusteringResultPrev, vectorSpace)
          proceed = Silhouette.compare(actualMetric, previousClusteringResultMetric)
          monitoredStatistics = monitoredStatistics.updateValue(MonitoringProperty.TIME_FOR_TEST_RESULT, System.currentTimeMillis() - time)
          LOGGER.debug(s"Actual Silhouette without the ${worstCluster.get.toString} was $previousClusteringResultMetric, can proceed $proceed.")
        }

        if(!proceed) {
          debugSeq :+= false
          reclustering(t, window)
          compute = false
          reclusteringMade = true
        } else {
          // update result
          clusteringResult = newClusteringResult
          clusteringResultPrev = clusteringResult
          updateHashTableFromNewClusters(newClusters)
          worstCluster = findWorstCluster(t, window, maxNumberOfClusters, worstCluster)
          debugSeq :+= true
        }
        debugOperations :+= debugSeq
      }
      DSCDebugOperations ++= debugOperations

      if(!reclusteringMade && useNewVersion) {
        //check the reclustering policy
        reclusteringRule(t, window, oldClusteringResultSilhouette)
      }
    }
  }

  /**
   * Perform reclustering
   * @param t actual time
   * @param window actual window
   */
  private def reclustering(t: Long, window: Window): Unit = {
    LOGGER.info("Perform reclustering")
    firstClusterGeneration(t, window)
    monitoredStatistics = monitoredStatistics.updateValue(MonitoringProperty.NUMBER_OF_RECLUSTERING, 1)
  }

  /**
   * Perform reclustering only when the difference between oldSS and actualSS is greater than the $worstCaseSSDecrese (1/number of panes)
   * @param t                   actual time
   * @param window              actual window
   * @param oldClusteringResultSilhouette the old clustering result SS
   */
  private def reclusteringRule(t: Long, window: Window, oldClusteringResultSilhouette: Double): Unit = {
    val time = System.currentTimeMillis()
    val actualSS = Silhouette.compute(clusteringResult, vectorSpace)
    monitoredStatistics = monitoredStatistics.updateValue(MonitoringProperty.TIME_FOR_TEST_RESULT, System.currentTimeMillis() - time)

    if ((oldClusteringResultSilhouette - actualSS) > worstCaseSSDecrese) {
      LOGGER.info(s"Reclustering rule satisfied (actualSS = $actualSS, oldSS = $oldClusteringResultSilhouette, worstCaseSSDecrese = $worstCaseSSDecrese)")
      reclustering(t, window)
    }
  }

  /**
   * @param t the execution time
   * @param window the analyzed window
   * @param maxNumberOfClusters the maximum number of cluster to stop splitting
   * @return the operation choose with the cluster(s) for the operation. If not present None
   */
  private def findWorstCluster(t: Long, window: Window, maxNumberOfClusters: Int, previousOperation: Option[DSCOperation]): Option[DSCOperation] = {
    var time = System.currentTimeMillis()
    scattering = clusteringResult.map{case (c, vx) => c -> clusterScattering(vx)}

    val lastOperationWasMerge = if(previousOperation.nonEmpty) { //flag for avoid to split after a merge
      previousOperation.get.isInstanceOf[MERGE]
    } else false

    scatteringZscore = if(lastOperationWasMerge) Map()  else calculateZScores(scattering)
    monitoredStatistics = monitoredStatistics.updateValue(MonitoringProperty.TIME_FOR_CALCULATE_FOR_SPLIT_OPERATION, System.currentTimeMillis() - time)

    val maximumValForSplit = util.Try(scatteringZscore.collect {
      case (c, score) if score > localReclustering.get.splitThreshold(clusteringResult.size) && clusteringResult(c).size >= 2 => (c, score) //add check for min number of records for split
    }.maxBy(_._2)).toOption

    val result = if(maximumValForSplit.nonEmpty && clusteringResult.size <= maxNumberOfClusters) {
      LOGGER.debug(s"Worst cluster SPLIT (th = ${localReclustering.get.splitThreshold(clusteringResult.size)}) is ${maximumValForSplit.get._1.centroidSchema.value} " +
        s"zscore avg dist = ${maximumValForSplit.get._2}")

      Some(SPLIT(maximumValForSplit.get._1, maximumValForSplit.get._2))
    } else {
      time = System.currentTimeMillis()
      calculateSeparationZScore()

      monitoredStatistics = monitoredStatistics.updateValue(MonitoringProperty.TIME_FOR_CALCULATE_FOR_MERGE_OPERATION, System.currentTimeMillis() - time)

      val mergeReduceClusters = clusteringResult.size > maxNumberOfClusters
      val maximumValueForMerge = (if(mergeReduceClusters) {
        //different merge rule in case the number of clusters is exceeded
        separationZScores.collect {
          case ((c1, c2), zScore) => ((c1, c2), zScore, overlapping((c1, c2)))
        }
      } else {
        separationZScores.collect {
          case ((c1, c2), zScore) if zScore < localReclustering.get.mergeThreshold(clusteringResult.size) => ((c1, c2), zScore, overlapping((c1, c2)))
        }.filter(_._3 >= localReclustering.get.overlapThreshold)
      }).toSeq.sortBy(x => (x._2, -x._3)).headOption

      if(maximumValueForMerge.isDefined) {
        LOGGER.debug(s"Worst cluster MERGE (th = ${localReclustering.get.mergeThreshold(clusteringResult.size)}) are ${maximumValueForMerge.get._1._1.centroidSchema.value} and ${maximumValueForMerge.get._1._2.centroidSchema.value}" +
          s"zscore merge = ${maximumValueForMerge.get._2} radii/distance = ${maximumValueForMerge.get._3}")

        Some(MERGE(maximumValueForMerge.get._1, zScore = maximumValueForMerge.get._2, radiiVsDistance = maximumValueForMerge.get._3, mergeReduceClusters))
      } else {
        LOGGER.debug("No worst cluster")
        None
      }
    }
    val totalTime = System.currentTimeMillis() - time
    monitoredStatistics = monitoredStatistics.updateValue(MonitoringProperty.TIME_FOR_FIND_CLUSTER_TO_SPLIT_MERGE, totalTime)
      .updateValue(MonitoringProperty.TIME_FOR_TEST_RESULT, totalTime)
      .updateValue(MonitoringProperty.NUMBER_OF_FIND_CLUSTER_TO_SPLIT_MERGE, 1)
    result
  }

  /**
   * Calculate clustering separation z-score and update of local variables
   */
  private def calculateSeparationZScore(): Unit = {
    val r = clusteringSeparationZScore(clusteringResult, scattering)
    separationZScores = r._1
    separation = r._2
    overlappingZScores = r._3
    overlapping = r._4
  }

  /**
   * Update the clustering adding best number of clusters (from 0 to sqrt) with data in the insert buffer
   * @param t computation time
   * @param window the examined window
   * @param correspondenceCentroids correspondence map between old clusters and new clusters
   */
  private def clusterInsertBucket(t: Long, window: Window, correspondenceCentroids: TwoPhasesCorrespondenceCentroids): Unit = {
    LOGGER.info(s"Produce best clusters with insert bucket (size = ${insertBucket.size})")
    val matchFromClusteringResult = insertBucket.groupBy(_._2).map {
      case (feature, map) => //use correspondence map to get the newest centroid value
        correspondenceCentroids(feature).getOrElse(feature) -> map.keys.toSeq
    }
    val result = LocalReclusteringUtils.addNewClusterAndValidateTheGlobalSolution(0, math.sqrt(insertBucket.size).floor.toInt, insertBucket.keys.toSeq,
      clusteringResult, matchFromClusteringResult, vectorSpace, randomInitOfClustering, metric, elbowSensitivity = elbowSensitivity)

    if(result.nonEmpty) {
      //"algorithm", "t", "windowStart", "windowEnd", "operation", "c1", "c2", "zScore", "RadiiVsDistances", "numberOfNewClusters", "opSuccess"
      DSCDebugOperations ++= result.get._2.map(x => Seq(name, t, window.start.getTime, window.end.getTime, INSERT, x._1.centroidSchema.value, null, null, null, 1, true)).toSeq
      clusteringResult = result.get._1
      updateHashTableFromNewClusters(result.get._2)
      insertBucket = Map()
    } else {
      LOGGER.error("Error of new insert buckets")
    }
  }

  /**
   * Update hash table using new clusters assignments.
   *
   * @param newClusters new clusters
   */
  private def updateHashTableFromNewClusters(newClusters: Map[GroupedGroupFeature, Seq[(GroupedGroupFeature, Double)]]): Unit = {
    val time = System.currentTimeMillis()
    newClusters.foreach {
      case (centroid, values) =>
        values.map(_._1).foreach {
          case x: BucketWindowGroupFeature[Int] => hashTable += x.bucket -> (x, Some(centroid))
        }
    }
    monitoredStatistics = monitoredStatistics.updateValue(MonitoringProperty.TIME_FOR_UPDATE_DATA_STRUCTURE, System.currentTimeMillis() - time)
  }

  /**
   * Add a new element to the cluster
   * @param window the window
   * @param gf a group feature
   * @param firstIterationWithOneCluster true if is the first iteration with one cluster
   * @return the cluster where the gf can be added
   */
  override def addElementToCluster(window: Window, gf: BucketWindowGroupFeature[Int], firstIterationWithOneCluster: Boolean): Option[GroupedGroupFeature] = {
    if(firstIterationWithOneCluster) {
      //create a new cluster and update the tables
      val newCentroid = GroupedGroupFeatureAggregator.center(Seq(gf)).asInstanceOf[GroupedGroupFeature]
      val newClusters = Map(newCentroid -> Seq(gf -> 0D))
      clusteringResult ++= newClusters
      oldRadius += newCentroid -> 0D
      updateHashTableFromNewClusters(newClusters)
      None
    } else {
      if (localReclustering.isEmpty) {
        super.addElementToCluster(window, gf, firstIterationWithOneCluster)
      } else {
        super.addElementToCluster(window, gf, firstIterationWithOneCluster)
      }
    }
  }

  /**
   * Write statistics about z-score scattering, separation and overlap
   * @param t time of window
   * @param window window
   */
  def writeDSCStatistics(t: Long, window: Window): Unit = {
    writeDebugDSC(DSCDebugOperations)
    writeDebugDSCScatteringzScore(name, t, window, scatteringZscore, scattering, getValue)
    writeDebugDSCSeparationZScore(name, t, window, separationZScores = separationZScores, separation = separation, overlappingZScores = overlappingZScores, overlapping = overlapping, getValue = getValue)
  }
}
