package it.unibo.big.streamprofiling.algorithm.implementation.incremental.twophases

import it.unibo.big.streamprofiling.algorithm.implementation.incremental.groupfeature.GroupedGroupFeature
import it.unibo.big.streamprofiling.algorithm.utils.Math._

import scala.collection.immutable.ListMap

/**
 * Object for TwoPhases algorithm data types
 */
private[twophases] object TwoPhasesDataTypes {

    import it.unibo.big.streamprofiling.algorithm.implementation.incremental.groupfeature.BucketWindowGroupFeature

    import scala.collection.immutable.SortedMap

  type TwoPhasesClusteringResult = Map[GroupedGroupFeature, Seq[(GroupedGroupFeature, Double)]]

  type TwoPhasesHashTable[T] = SortedMap[IndexedSeq[T], (BucketWindowGroupFeature[T], Option[GroupedGroupFeature])]

  type TwoPhasesClusterAssignments[T] = Map[GroupedGroupFeature, Seq[BucketWindowGroupFeature[T]]]

  type TwoPhasesDictionary = ListMap[String, Long]

  type TwoPhasesCorrespondenceCentroids = Map[GroupedGroupFeature, Option[GroupedGroupFeature]]

  /**
   * Trait for the operation of the DSC algorithm
   */
  sealed trait DSCOperation

  /**
   *
   * @param cluster cluster to split
   * @param zScore of split (scattering)
   */
  case class SPLIT(cluster: GroupedGroupFeature, zScore: Double) extends DSCOperation {
    override def toString: String = "SPLIT"
  }

  /**
   *
   * @param cluster cluster to fadeout
   */
  case class FADEOUT(cluster: GroupedGroupFeature) extends DSCOperation {
    override def toString: String = "FADEOUT"
  }

  /**
   *
   * @param clusters clusters pair to merge
   * @param zScore of merge (separation)
   * @param radiiVsDistance overlap of merge
   * @param mergeForReduceK true if the merge is for reduce k to be in the range of sqrt of number of records
   */
  case class MERGE(clusters: (GroupedGroupFeature, GroupedGroupFeature), zScore: Double, radiiVsDistance: Double, mergeForReduceK: Boolean) extends DSCOperation {
    override def toString: String = "MERGE"
  }

  /**
   * Insert operation
   */
  case object INSERT extends DSCOperation
}


/**
 * Object for TwoPhases algorithm utils
 */
private [twophases] object TwoPhasesClusteringUtils {

  import TwoPhasesDataTypes._

  /**
   *
   * @param cluster an input cluster as map of values, distances from the centroid
   * @return the radius of the cluster, the maximum distance from centroid considering the number of element of a GF
   */
  def clusterRadius(cluster: Seq[(GroupedGroupFeature, Double)]): Double = {
    if(cluster.isEmpty) 0.0 else cluster.map{
      case (v, d) => d / v.N
    }.max
  }

  /**
   *
   * @param cluster an input cluster as map of values, distances from the centroid
   * @return average distance of the cluster elements from the centroid (i.e., scattering)
   */
  def clusterScattering(cluster: Seq[(GroupedGroupFeature, Double)]): Double = {
    val distances = obtainDistancesFromClusterElements(cluster)
    distances.sum / distances.size
  }

  /**
   * Obtain the distances from the cluster elements
   * @param cluster an input cluster as map of values, distances from the centroid
   * @return the distances from the cluster elements
   */
  def obtainDistancesFromClusterElements(cluster: Seq[(GroupedGroupFeature, Double)]): Seq[Double] = {
    cluster.flatMap {
      case (e, d) => Seq.fill(e.N)(d / e.N)
    }
  }

  /**
   *
   * @param xs a map that for each cluster has a measure (e.g. average cluster distance)
   * @return for each cluster the z-score for split (scattering)
   */
  def calculateZScores[T](xs: Map[T, Double]): Map[T, Double] = {
    val avgXs = median(xs.values.toSeq)
    val stdDevXs = mad(xs.values.toSeq)
    xs.map{
      case (c, v) => c -> robustZScore(v, avgXs, stdDevXs)
    }
  }

  /**
   *
   * @param clustering a clustering result, for TwoPhase algorithm
   * @param scattering the clusters scattering
   * @return
   *  - separation zscores and separation
   *  - overalapping zscores and overlapping
   */
  def clusteringSeparationZScore(clustering: TwoPhasesClusteringResult, scattering: Map[GroupedGroupFeature, Double]): (
    Map[(GroupedGroupFeature, GroupedGroupFeature), Double],
    Map[(GroupedGroupFeature, GroupedGroupFeature), Double],
    Map[(GroupedGroupFeature, GroupedGroupFeature), Double],
    Map[(GroupedGroupFeature, GroupedGroupFeature), Double]) = {
    val clustersWithIndex = clustering.keys.zipWithIndex.map { case (c, i) => (i, (c, c.centroidSchema)) }.toMap
    val separation = clustersWithIndex.flatMap {
      case (i, (x, centroidX)) => clustersWithIndex.filterKeys(_ > i).flatMap {
        case (_, (y, centroidY)) =>
          val distance = centroidX.jaccard(centroidY)
          Map(((x, y), distance), ((y, x), distance))
      }
    }
    getScatteringAndSeparationZScores(separation, scattering)
  }

  /**
   * Get the z-scores for separation and overlapping
   * @tparam T type of the elements
   * @param separation separation map
   * @param scattering scattering map
   * @return z-scores for separation and overlapping
   */
  def getScatteringAndSeparationZScores[T](separation: Map[(T, T), Double], scattering: Map[T, Double]): (Map[(T, T), Double], Map[(T, T), Double], Map[(T, T), Double], Map[(T, T), Double]) = {
    val separationZScores = calculateZScores(separation)

    val overlapping = separation.map {
      case ((f1, f2), v) => (f1, f2) -> (scattering(f1) + scattering(f2)) / v
    }
    val overlappingZScores = calculateZScores(overlapping)

    (separationZScores, separation, overlappingZScores, overlapping)
  }
}