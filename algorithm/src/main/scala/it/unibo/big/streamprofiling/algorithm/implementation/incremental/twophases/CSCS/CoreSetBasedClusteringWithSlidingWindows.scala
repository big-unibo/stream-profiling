package it.unibo.big.streamprofiling.algorithm.implementation.incremental.twophases.CSCS

import it.unibo.big.streamprofiling.algorithm.Metrics.EvaluationMetric
import it.unibo.big.streamprofiling.algorithm.implementation.incremental.IncrementalClusteringUtils.reclusteringIfNeeded
import it.unibo.big.streamprofiling.algorithm.implementation.incremental.groupfeature.{GroupedGroupFeature, SimplifiedGroupFeature}
import it.unibo.big.streamprofiling.algorithm.implementation.incremental.twophases.AbstractTwoPhasesClustering
import it.unibo.big.streamprofiling.algorithm.implementation.incremental.twophases.TwoPhasesDataTypes.{TwoPhasesClusteringResult, TwoPhasesCorrespondenceCentroids, TwoPhasesDictionary}
import it.unibo.big.streamprofiling.algorithm.implementation.incremental.twophases.reducecoreset.ReduceCoresetTypes.{ReduceCoresetType, STANDARD}
import it.unibo.big.streamprofiling.algorithm.utils.MonitoringProperty
import it.unibo.big.utils.input.SchemaModeling.SchemaWithTimestamp

/**
 * Implementation of the clustering explained in "Efficient Data Stream Clustering With Sliding Windows Based on Locality-Sensitive Hashing"
 *
 * <a>https://ieeexplore.ieee.org/document/8501907</a>
 *
 * @param name clustering name string representation
 * @param windowLength length of the window
 * @param windowPeriod length of the period
 * @param m coreset size
 * @param l number of hash functions
 * @param w same as threshold, chosen with a predefined process, depending on the dataset. If not provided just try to estimate
 * @param metric the metric for evaluate the best k, number of cluster
 * @param applyReduceBucketHeuristic true if you want to apply the heuristic to reduce bucket numbers, avoiding comparing the distances of all buckets (default true)
 * @param randomInitOfClustering true if random init, false if kmeans++ (default true)
 * @param reduceCoresetType type of applied to reduce coreset implementation (default STANDARD, i.e. Manhattan)
 * @param elbowSensitivity     the sensitivity for elbow method, needed when the metric used is the SSE
 * @param maximumNumberOfClusters maximumNumberOfCluster in OMRk
 */
private [algorithm] class CoreSetBasedClusteringWithSlidingWindows(name: String, windowLength: Long, windowPeriod: Long, m: Int, l: Int, w: Option[Double], metric: EvaluationMetric, applyReduceBucketHeuristic: Boolean = true,
                                                                   randomInitOfClustering: Boolean = false, reduceCoresetType: ReduceCoresetType = STANDARD, elbowSensitivity: Option[Double], maximumNumberOfClusters: Option[Int])
                                extends AbstractTwoPhasesClustering[Int](windowLength, windowPeriod, m, l, applyReduceBucketHeuristic, randomInitOfClustering, metric, reduceCoresetType, elbowSensitivity, maximumNumberOfClusters) {

  import it.unibo.big.streamprofiling.algorithm.implementation.incremental.IncrementalClusteringUtils.clusteringExtractorGroupFeature
  import it.unibo.big.streamprofiling.algorithm.implementation.incremental.groupfeature.GroupFeatureVectorSpace.vectorGroupedSpace
  import it.unibo.big.streamprofiling.algorithm.implementation.incremental.twophases.Debug2PhasesAlgorithmsUtils._
  import it.unibo.big.streamprofiling.algorithm.implementation.kmeans.KMeans.kmeans
  import it.unibo.big.streamprofiling.algorithm.utils.RandomUtils
  import it.unibo.big.streamprofiling.algorithm.utils.VectorSpace.VectorSpace
  import it.unibo.big.utils.input.SchemaModeling.{Schema, Window}

  import scala.collection.immutable.ListMap

  private val RANDOM_SAMPLES_PERCENTAGE = 0.5D //percentage of random sample for parameters estimation, from thesis

  private var actualDictionary: TwoPhasesDictionary = ListMap()
  private var euclideanW: Double = 0D
  private var hashes = Seq[Schema => Int]()

  /**
   *
   * @param seq the input schemas of first iteration (B)
   */
  override def firstIterationFunction(seq: Seq[Schema]): Unit = {
    if(w.isDefined) {
      LOGGER.info("Set w with parameter")
      euclideanW = w.get
    } else {
      LOGGER.info("Set w with process")
      val sampleSize = (seq.size * RANDOM_SAMPLES_PERCENTAGE).toInt
      val sample = RandomUtils.pickRandom(seq, if (sampleSize > 0) sampleSize else 1, random = randomInitOfClustering)
      val b1 = seq.head //initial tuple
      //minimum distance between b1 and random samples D from B
      val distances = sample.map(x => x.euclideanDistance(b1))
      euclideanW = distances.filter(_ > 0).min
    }
    LOGGER.info(s"Set w = $euclideanW")
  }

  /**
   *
   * @param s the input schema
   * @return the hash value of the input schemas as concatenation of several hash results
   */
  private[twophases] def g(s: Schema): IndexedSeq[Int] = hashes.map(_(s)).toIndexedSeq

  /**
   *
   * @param newSchemas the new schemas to hash
   * @return the hash values of the new schemas
   */
  override def applyHash(newSchemas: Seq[(Schema, Long)]): Map[IndexedSeq[Int], Map[Long, SimplifiedGroupFeature]] = newSchemas.map {
      case (s, t) => g(s) -> (t -> s)
    }.groupBy(_._1).map {
      case (hash, xs) => hash -> xs.groupBy(_._2._1).map {
        case (t, tuples) => t -> SimplifiedGroupFeature(t, tuples.map(_._2._2))
      }
    }

  /**
   * Update the hash functions with a new dictionary
   * @param newDictionary the new dictionary values
   */
  private def updateHashFunctions(newDictionary: TwoPhasesDictionary): Unit = {
    actualDictionary = newDictionary
    hashes = (1 to l).map(i => {
      val hash = new EuclideanHash(actualDictionary.keys.toIndexedSeq, euclideanW, i)
      (s: Schema) => hash.hash(s)
    })
  }

  /**
   * Restart buckets, initialize new function with a new dictionary and insert the new hash values as Buckets
   *
   * @param newDictionary the new dictionary values, different from the previous one
   * @param schemas       the schemas to bucketize with timestamp
   * @param t             the time of computation
   * @param window        window of computation
   */
  private def updateHashesWithNewDictionary(newDictionary: TwoPhasesDictionary, schemas: Seq[(Schema, Long)], t: Long, window: Window): Unit = {
    LOGGER.info("Dictionary change, update of hash functions")
    val t = System.currentTimeMillis()
    var changeDict = false
    if (actualDictionary.nonEmpty) {
      changeDict = true
    }
    monitoredStatistics = monitoredStatistics.updateValue(MonitoringProperty.NUMBER_OF_CHANGE_DICTIONARY, 1)
    updateHashFunctions(newDictionary)
    hashTable = hashTable.empty
    getHashOfSchemas(schemas)
    if (changeDict) {
      val time = System.currentTimeMillis()
      val clusteringRecords = hashTable.values.map(_._1).toSeq
      clusteringResult = kmeans(clusteringRecords, clusteringResult.size, vectorSpace, randomInitOfClustering, toCenter = clusteringExtractorGroupFeature.toCenter)
      updateHashTable()
      val clusteringTotalTime = System.currentTimeMillis() - time
      monitoredStatistics = monitoredStatistics.updateValue(MonitoringProperty.TIME_FOR_CLUSTERING, clusteringTotalTime)
      operationDebug :+= OperationDebugInfo(CLUSTERING_FROM_K, clusteringRecords.size, clusteringTotalTime)
    } else {
      clusteringResult = Map()
      firstClusterGeneration(t, window)
    }
    monitoredStatistics = monitoredStatistics.updateValue(MonitoringProperty.TIME_FOR_CHANGE_DICTIONARY, System.currentTimeMillis() - t)
  }

  /**
   *
   *
   * @param newSchemas new schemas to insert
   * @param t the time of computation
   * @param window window of computation
   * @return true if there is the need to an incremental update of the result, false otherwise.
   *         In case it return false it is supposed the result and the hash table will be changed
   */
  override def incrementalUpdateOfResult(newSchemas: Seq[SchemaWithTimestamp], window: Window, t: Long): Boolean = {
    val newDataDictionary = newSchemas.flatMap(x => x.values.keySet.map(v => v -> x.timestamp.getTime))
      .groupBy(_._1).map{
        case (a, values) => a -> values.map(_._2).max
      }
    val validDataDictionary = ListMap(actualDictionary.collect {
      case (s, _) if newDataDictionary.contains(s) => (s, newDataDictionary(s))
      case (s, ti) if window.contains(ti) => (s, ti)
    }.toSeq : _*)

    if (newDataDictionary.keys.forall(validDataDictionary.contains)) {
      // if the new data dictionary is equal to the valid one, then we can update the hash table
      if (validDataDictionary.size == actualDictionary.size) {
        actualDictionary = validDataDictionary
        true
      } else {
        updateHashesWithNewDictionary(validDataDictionary, timedSchema, t, window)
        false
      }
    } else {
      updateHashesWithNewDictionary(validDataDictionary ++ newDataDictionary, timedSchema, t, window)
      false
    }
  }

  /**
   *
   * @return the vector space for the clustering considering Euclidean distance
   */
  override val vectorSpace: VectorSpace[GroupedGroupFeature] = vectorGroupedSpace((s1, s2) => s1.euclideanDistance(s2))

  /**
   * Check if there is the need to recluster the data, if so update the clustering result
   *
   * @param oldClusteringResult the previous clustering result
   * @param correspondenceCentroids the correspondence centroids
   * @param window the actual window
   * @param t the time of computation
 */
  override private[twophases] def checkReclustering(oldClusteringResult: TwoPhasesClusteringResult, correspondenceCentroids: TwoPhasesCorrespondenceCentroids, t: Long, window: Window): Unit = {
    val time = System.currentTimeMillis()
    val (reclusteringResult, reclusteringFlag, newStatistics) = reclusteringIfNeeded(windowLength, windowPeriod, clusteringResult, oldClusteringResult, correspondenceCentroids, t, window, vectorSpace, randomInitOfClustering, metric, maxNumberOfClusters = maximumNumberOfClusters.getOrElse(m), elbowSensitivity = elbowSensitivity, statistics = monitoredStatistics)
    monitoredStatistics = newStatistics
    if (reclusteringFlag) {
      clusteringResult = reclusteringResult
      updateHashTable()
      operationDebug :+= OperationDebugInfo(CLUSTERING, clusteringResult.values.map(_.size).sum, System.currentTimeMillis() - time)
    }
  }

  /**
   *
   * @return the clustering result as a map of group features where each one is a cluster and the value is the list of elements in the cluster with the distance from the centroid
   */
  def groupedFeatureClusteringResult: Map[GroupedGroupFeature, Seq[(GroupedGroupFeature, Double)]] = clusteringResult
}
