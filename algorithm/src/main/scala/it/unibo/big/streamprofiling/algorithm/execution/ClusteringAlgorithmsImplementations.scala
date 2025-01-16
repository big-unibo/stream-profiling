package it.unibo.big.streamprofiling.algorithm.execution

import it.unibo.big.streamprofiling.algorithm.Metrics
import it.unibo.big.streamprofiling.algorithm.implementation.incremental.twophases.reducecoreset.ReduceCoresetTypes
import it.unibo.big.streamprofiling.algorithm.implementation.incremental.twophases.reducecoreset.ReduceCoresetTypes.{CENTROID, ReduceCoresetType, STANDARD}

/**
 * Utility for defining algorithms parameters that can be passed before execution
 */
object ClusteringAlgorithmsImplementations {

  import ClusteringAlgorithms.ClusteringAlgorithm
  import Metrics.{EvaluationMetric, SSE, Silhouette}

  import scala.util.matching.Regex

  /**
   * case class for extra clustering name not categorized
   *
   * @param name of the method
   */
  case class Clustering(name: String) extends ClusteringAlgorithm {
    override def toString: String = name
  }

  sealed trait IncrementalClusteringAlgorithm extends ClusteringAlgorithm

  /**
   *
   * @param tauMerge lower bound of z-score for merge operation
   * @param tauSplit upper bound of z-score for split operation
   * @param insertPercentageDistance delta for allow insert of a gf in a cluster
   */
  case class LocalReclustering(tauMerge: Int, tauSplit: Int, overlapThreshold: Double, insertPercentageDistance: Double) {
    def mergeThreshold(k: Int): Double =  Math.log10(k) * tauMerge
    def splitThreshold(k: Int): Double =  Math.log10(k) * tauSplit
  }

  /**
   *
   * @param numberOfHashFunction number of hash function for coreset construction -- 5 to 25 (default 15)
   * @param hashTableMaxSize     maximum number of coreset (m)
   * @param metric               metric to choose best k in clustering
   * @param reduceCoresetType    implementation of reduce corset (default centroid)
   * @param localReclustering    local reclustering parameters
   * @param useNewVersion true for new version, if you use the heurstic to reduce comparisons in bucket reducing and k=2 in split
   * @param maximumNumberOfClusters maximumNumberOfCluster in OMRk
   */
  case class DSC(numberOfHashFunction: Int, hashTableMaxSize: Int, metric: EvaluationMetric,
                 reduceCoresetType: ReduceCoresetType = CENTROID, localReclustering: Option[LocalReclustering],
                 useNewVersion: Boolean = true, elbowSensitivity: Option[Double] = None, maximumNumberOfClusters: Option[Int] = None) extends IncrementalClusteringAlgorithm {
    override val parameters: String = "numberOfHashFunction,hashTableMaxSize,metric,reduceCoreset,tau_merge,tau_split,overlapThreshold,insertDistanceThreshold,+,elbowSensitivity,maximumNumberOfClusters"

    override def toString: String = localReclustering match {
      case Some(LocalReclustering(tauMerge, tauSplit, overlapThreshold, insertPercentageDistance)) =>
        s"DSC($numberOfHashFunction,$hashTableMaxSize,$metric,$reduceCoresetType,$tauMerge,$tauSplit,$overlapThreshold,$insertPercentageDistance,$useNewVersion,${elbowSensitivity.getOrElse("")},${maximumNumberOfClusters.getOrElse("")})"
      case None => s"DSC($numberOfHashFunction,$hashTableMaxSize,$metric,$reduceCoresetType,_,_,_,_,$useNewVersion,${elbowSensitivity.getOrElse("")},${maximumNumberOfClusters.getOrElse("")})"
    }
  }

  object DSC {
    private val DSCRegex: Regex = "(.*)DSC\\((\\d+),(\\d+),(SSE|Silhouette)\\)(.*)".r
    private val DSCRegex1: Regex = "(.*)DSC\\((\\d+),(\\d+),(SSE|Silhouette),(CENTROID|STANDARD|HASH_LENGTH|HAMMING|JACCARD)\\)(.*)".r
    private val DSCRegex3: Regex = "(.*)DSC\\((\\d+),(\\d+),(SSE|Silhouette),(CENTROID|STANDARD|HASH_LENGTH|HAMMING|JACCARD),([+-]?\\d+),([+-]?\\d+),(0(?:\\.\\d+)?|1(?:\\.0+)?),(0(?:\\.\\d+)?|1(?:\\.0+)?),(.*)\\)(.*)".r
    private val DSCRegex4: Regex = "(.*)DSC\\((\\d+),(\\d+),(SSE|Silhouette),(CENTROID|STANDARD|HASH_LENGTH|HAMMING|JACCARD),(true|false),([+-]?\\d+),([+-]?\\d+),(0(?:\\.\\d+)?|1(?:\\.0+)?),(0(?:\\.\\d+)?|1(?:\\.0+)?),(.*)\\)(.*)".r
    private val DSCRegex5: Regex = "(.*)DSC\\((\\d+),(\\d+),(SSE|Silhouette),(CENTROID|STANDARD|HASH_LENGTH|HAMMING|JACCARD),(true|false),([+-]?\\d+),([+-]?\\d+),(0(?:\\.\\d+)?|1(?:\\.0+)?),(0(?:\\.\\d+)?|1(?:\\.0+)?),(\\d+)\\)(.*)".r

    def fromString(str: String): Option[DSC] = str match {
      case DSCRegex(_, hash, m, metric, _) => Some(DSC(hash.toInt, m.toInt, if (metric == "SSE") SSE else Silhouette, localReclustering = None))
      case DSCRegex1(_, hash, m, metric, reduceCoreset, _) => Some(DSC(hash.toInt, m.toInt, if (metric == "SSE") SSE else Silhouette, ReduceCoresetTypes.fromString(reduceCoreset), None))
      case DSCRegex5(_, hash, m, metric, reduceCoreset, heuristic, tauMerge, tauSplit, overlapThreshold, insertPercentageDistance, maxK, _) =>
        Some(DSC(hash.toInt, m.toInt, if (metric == "SSE") SSE else Silhouette, ReduceCoresetTypes.fromString(reduceCoreset),
          Some(LocalReclustering(tauMerge.toInt, tauSplit.toInt, overlapThreshold.toDouble, insertPercentageDistance.toDouble)),
          heuristic.toLowerCase == "true", elbowSensitivity = Some(1D), maximumNumberOfClusters = Some(maxK.toInt)))
      case DSCRegex4(_, hash, m, metric, reduceCoreset, heuristic, tauMerge, tauSplit, overlapThreshold, insertPercentageDistance, elbowSensitivity, _) =>
        Some(DSC(hash.toInt, m.toInt, if (metric == "SSE") SSE else Silhouette, ReduceCoresetTypes.fromString(reduceCoreset),
          Some(LocalReclustering(tauMerge.toInt, tauSplit.toInt, overlapThreshold.toDouble, insertPercentageDistance.toDouble)), heuristic.toLowerCase == "true", elbowSensitivity = Some(elbowSensitivity.toDouble)))
      case DSCRegex3(_, hash, m, metric, reduceCoreset, tauMerge, tauSplit, overlapThreshold, insertPercentageDistance, elbowSensitivity, _) =>
        Some(DSC(hash.toInt, m.toInt, if (metric == "SSE") SSE else Silhouette, ReduceCoresetTypes.fromString(reduceCoreset),
          Some(LocalReclustering(tauMerge.toInt, tauSplit.toInt, overlapThreshold.toDouble, insertPercentageDistance.toDouble)), elbowSensitivity = Some(elbowSensitivity.toDouble)))

      case _ => None
    }
  }

  /**
   *
   * @param numberOfHashFunction number of hash function for coreset construction -- 5 to 25 (default 15)
   * @param hashTableMaxSize     maximum number of coreset (m)
   * @param tolerance            tolerance for coreset construction (if empty calculated with predefined process)
   * @param metric               metric to choose best k in clustering
   * @param reduceCoresetType    implementation of reduce corset (default standard)
   * @param maximumNumberOfClusters maximumNumberOfCluster in OMRk
   */
  case class CSCS(numberOfHashFunction: Int, hashTableMaxSize: Int, tolerance: Option[Double], metric: EvaluationMetric,
                  reduceCoresetType: ReduceCoresetType = STANDARD, elbowSensitivity : Option[Double] = None,
                  maximumNumberOfClusters: Option[Int] = None) extends IncrementalClusteringAlgorithm {
    override val parameters: String = "numberOfHashFunction,hashTableMaxSize,tolerance,metric,reduceCoreset,elbowSensitivity,maximumNumberOfClusters"
  }

  object CSCS {
    private val CSCSRegex: Regex = "(.*)CSCS\\((\\d+),(\\d+),(.*),(SSE|Silhouette)\\)(.*)".r
    private val CSCSRegex2: Regex = "(.*)CSCS\\((\\d+),(\\d+),(SSE|Silhouette)\\)(.*)".r
    private val CSCSRegex3: Regex = "(.*)CSCS\\((\\d+),(\\d+),(.*),(SSE|Silhouette),(CENTROIDEU|STANDARD|HASH_LENGTH|HAMMING|JACCARD)\\)(.*)".r
    private val CSCSRegex4: Regex = "(.*)CSCS\\((\\d+),(\\d+),(SSE|Silhouette),(CENTROIDEU|STANDARD|HASH_LENGTH|HAMMING|JACCARD)\\)(.*)".r
    private val CSCSRegex5: Regex = "(.*)CSCS\\((\\d+),(\\d+),(SSE),(.*)\\)(.*)".r
    private val CSCSRegex6: Regex = "(.*)CSCS\\((\\d+),(\\d+),(SSE|Silhouette),(\\d+)\\)(.*)".r

    def fromString(str: String): Option[CSCS] = str match {
      case CSCSRegex(_, hash, m, tol, metric, _) => Some(CSCS(hash.toInt, m.toInt, Some(tol.toDouble), if (metric == "SSE") SSE else Silhouette))
      case CSCSRegex2(_, hash, m, metric, _) => Some(CSCS(hash.toInt, m.toInt, None, if (metric == "SSE") SSE else Silhouette))
      case CSCSRegex3(_, hash, m, tol, metric, reduceCoreset, _) => Some(CSCS(hash.toInt, m.toInt, Some(tol.toDouble), if (metric == "SSE") SSE else Silhouette, ReduceCoresetTypes.fromString(reduceCoreset)))
      case CSCSRegex4(_, hash, m, metric, reduceCoreset, _) => Some(CSCS(hash.toInt, m.toInt, None, if (metric == "SSE") SSE else Silhouette, ReduceCoresetTypes.fromString(reduceCoreset)))
      case CSCSRegex6(_, hash, m, metric, maxK, _) => Some(CSCS(hash.toInt, m.toInt, None, if (metric == "SSE") SSE else Silhouette, elbowSensitivity = if(metric =="SSE") Some(1D) else None, maximumNumberOfClusters = Some(maxK.toInt)))
      case CSCSRegex5(_, hash, m, _, elbowSensitivity, _) => Some(CSCS(hash.toInt, m.toInt, None, SSE, elbowSensitivity = Some(elbowSensitivity.toDouble)))
      case _ => None
    }
  }

  /**
   *
   * @param tentative identifier of OMRk++ simulation
   * @param kMax a function that given a number of records return the maximum value of k to try starting from 2.
   *             For SSE is a costant, for Silhouette is the sqrt of records
   * @param metric used evaluation metric (SSE with elbow or maximum SS)
   */
  case class OMRkpp(tentative: Option[String], kMax: Int => Int, metric: EvaluationMetric, elbowSensitivity: Option[Double]) extends ClusteringAlgorithm {
    if(metric == SSE) require(elbowSensitivity.isDefined) else require(elbowSensitivity.isEmpty) //sensitivity is needed just for SSE
    override val parameters: String = "tentative,metric,elbowSensitivity"
    override def toString: String = s"OMRkpp(${tentative.getOrElse("noname")},$metric,${elbowSensitivity.getOrElse("")})"
  }

  object OMRkpp {
    private val OMRkppRegex: Regex = "(.*)OMRkpp\\((\\d+),(Silhouette|SSE)\\)(.*)".r
    private val OMRkppRegex_tentative: Regex = "(.*)OMRkpp\\((\\d+),(\\d+),(Silhouette|SSE)\\)(.*)".r
    private val OMRkppRegex1: Regex =  """(.*)OMRkpp\((\d+),(SSE),(.*)\)(.*)""".r
     private val OMRkppRegex2: Regex = "(.*)OMRkpp\\(SSE,(.*)\\)(.*)".r

    def fromString(str: String): Option[OMRkpp] = str match {
      case OMRkppRegex(_, kMax, metric, _) =>
        val m = if(metric.toUpperCase == "SSE") SSE else Silhouette
        Some(OMRkpp(None, _ => math.max(2, kMax.toInt), m, elbowSensitivity = if(m == SSE) Some(1D) else None))
      case OMRkppRegex_tentative(_, tentative, kMax, metric, _) =>
        val m = if (metric.toUpperCase == "SSE") SSE else Silhouette
        Some(OMRkpp(Some(s"${tentative}_kmax$kMax"), _ => math.max(2, kMax.toInt), m, elbowSensitivity = if (m == SSE) Some(1D) else None))
      case OMRkppRegex1(_, kMax, _, elbowSensitivity, _) => Some(OMRkpp(None, _ => math.max(2, kMax.toInt), SSE, elbowSensitivity = Some(elbowSensitivity.toDouble)))
      case OMRkppRegex2(_, elbowSensitivity, _) => Some(OMRkpp(None, r => math.max(2, math.floor(math.sqrt(r)).toInt), SSE, elbowSensitivity = Some(elbowSensitivity.toDouble)))
      case "OMRkpp(Silhouette)" => Some(OMRkpp(None, r => math.max(2, math.floor(math.sqrt(r)).toInt), Silhouette, elbowSensitivity = None))
      case "OMRkpp(SSE)" => Some(OMRkpp(None, r => math.floor(math.sqrt(r)).toInt, SSE, elbowSensitivity = Some(1D)))
      case _ => None
    }
  }

  /**
   *
   * @param generateNumberOfPopulations       number of individual used for a population. In FEAC 4 or 20
   * @param kmeansIterations                  number of kmeans iteration in FEAC algorithm
   * @param percentatageOfElementsForMutation percentage of elements to keep for mutation and construct the next generation (an alternative is to choose random)
   * @param startingNumberOfClusters          starting number of clusters (k), optional -- for test purpose
   * @param maximumNumberOfClusters           maximum number of clusters (k), optional -- for test purpose
   */
  case class FEACS(generateNumberOfPopulations: Int, kmeansIterations: Int, percentatageOfElementsForMutation: Double, startingNumberOfClusters: Option[Int] = None, maximumNumberOfClusters: Option[Int] = None) extends IncrementalClusteringAlgorithm {
    override val parameters: String = "generateNumberOfPopulations,kmeansIterations,percentatageOfElementsForMutation,startingK,maximumNumberOfClusters"
  }

  object FEACS {
    private val FEACSRegex: Regex = "(.*)FEACS\\((\\d+),(\\d+),(.*)\\)(.*)".r
    private val FEACSRegex2: Regex = "(.*)FEACS\\((\\d+),(\\d+),(.*),(\\d+)\\)(.*)".r
    private val FEACSRegex3: Regex = "(.*)FEACS\\((\\d+),(\\d+),(\\d+),(.*)\\)(.*)".r

    def fromString(str: String): Option[FEACS] = str match {
      case FEACSRegex3(_, maxK, pop, kmeansIter, elemForMutation, _) => Some(FEACS(pop.toInt, kmeansIter.toInt, elemForMutation.toDouble, maximumNumberOfClusters = Some(maxK.toInt)))
      case FEACSRegex2(_, pop, kmeansIter, elemForMutation, startingNumberOfClusters, _) => Some(FEACS(pop.toInt, kmeansIter.toInt, elemForMutation.toDouble, startingNumberOfClusters = Some(startingNumberOfClusters.toInt)))
      case FEACSRegex(_, pop, kmeansIter, elemForMutation, _) => Some(FEACS(pop.toInt, kmeansIter.toInt, elemForMutation.toDouble))
      case _ => None
    }
  }
}
