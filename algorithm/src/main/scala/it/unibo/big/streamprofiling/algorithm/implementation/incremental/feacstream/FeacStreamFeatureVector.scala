package it.unibo.big.streamprofiling.algorithm.implementation.incremental.feacstream

import it.unibo.big.streamprofiling.algorithm.Metrics.SSResult
import it.unibo.big.streamprofiling.algorithm.implementation.incremental.feacstream.MutationOperations.MutationOperation
import it.unibo.big.streamprofiling.algorithm.implementation.incremental.groupfeature.SimplifiedGroupFeature
import it.unibo.big.utils.input.SchemaModeling.{ClusteringResult, Schema, SchemaWithTimestamp}

/**
 * Representation of FEAC Stream feature vector
 *
 * @param index the index of the feature vector, cluster index
 * @param timestamp the last timestamp
 * @param schemas   schemas in the group feature
 */
private [feacstream] class FeacStreamFeatureVector(val index: Int, timestamp: Long, schemas: Seq[Schema]) extends SimplifiedGroupFeature(timestamp, schemas)

/**
 * Representation of FEAC Stream clustering result
 *
 * @param clusters       the feature vectors
 * @param startingEncodingScheme the starting encoding scheme
 */
private [feacstream] case class FeacStreamClusteringResult(clusters: Map[Int, FeacStreamFeatureVector],
                                      startingEncodingScheme: Map[Long, IndexedSeq[Int]],
                                      startingSchemas: Map[Long, IndexedSeq[SchemaWithTimestamp]]) {
  private var encodingSchemeTmp = startingEncodingScheme
  private var schemasTmp = startingSchemas

  /**
   *
   * @return actual encoding scheme of clustering result
   */
  def encodingScheme: Map[Long, IndexedSeq[Int]] = encodingSchemeTmp

  /**
   *
   * @return actual schemas of clustering result, if saveRawData is set to true
   */
  def schemas: Map[Long, IndexedSeq[SchemaWithTimestamp]] = schemasTmp

  /**
   * Add the element into the result (i.e. in the candidateCluster and update encodingScheme)
   * @param candidateCluster the candidate cluster for adding the element
   * @param element element to add
   * @param t timestamp of the element
   */
  def add(candidateCluster: FeacStreamFeatureVector, element: SchemaWithTimestamp, t: Long, saveRawData: Boolean): Unit = {
    candidateCluster.add(element, t)
    encodingSchemeTmp += (t -> (encodingSchemeTmp.getOrElse(t, IndexedSeq()) :+ candidateCluster.index))
    if(saveRawData) {
      schemasTmp += (t -> (schemasTmp.getOrElse(t, IndexedSeq()) :+ element))
    }
  }

  /**
   *
   * @return true if the clusters is empty
   */
  def isEmpty: Boolean = clusters.isEmpty

  /**
   *
   * @return number of clusters, the last element of the encoding scheme
   */
  def k: Int = clusters.size
}

/**
 * Companion object of the FeacStreamClusteringResult case class
 */
private [feacstream] object FeacStreamClusteringResult {
  /**
   *
   * @return an empty FeacStreamClusteringResult
   */
  def apply(): FeacStreamClusteringResult = FeacStreamClusteringResult(Map(), Map(), Map())
}

/**
 * Traits for mutation operations, as defined in the reference paper
 */
private [feacstream] object MutationOperations {
  sealed trait MutationOperation
  sealed trait ExecutedMutationOperation extends MutationOperation
  case object MO1 extends ExecutedMutationOperation
  case object MO2 extends ExecutedMutationOperation

  /**
   * Fake mutation operation
   */
  case object NoMutation extends MutationOperation
}

/**
 * Representation of the individual in the FEAC Stream algorithm
 * @param actualIndex the actual index of the individual
 * @param clustering the actual clustering
 * @param ss the simplified silhouette
 * @param mutationOperation the applied mutation operator
 * @param pastGenerationIndex the past index of the individual in the previous generation
 * @param generation the number of generation of the individual
 */
private [feacstream] case class FeacIndividual(actualIndex: Int, clustering: ClusteringResult, ss: SSResult[Schema], mutationOperation: MutationOperation, pastGenerationIndex: Int, generation: Int)