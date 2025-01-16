package it.unibo.big.streamprofiling.algorithm.implementation.incremental.groupfeature

import it.unibo.big.streamprofiling.algorithm.utils.VectorSpace.VectorSpace
import it.unibo.big.utils.input.SchemaModeling.Schema

private[incremental] object GroupFeatureVectorSpace {
  /**
   *
   * The vector space for GF, with input distance function
   *
   * @param similarityFunction the distance function
   */
  def vectorGroupedSpace(similarityFunction: (Schema, Schema) => Double): VectorSpace[GroupedGroupFeature] = new VectorSpace[GroupedGroupFeature] {

    /**
     *
     * @param x an element in the vector space
     * @param y another element in the vector space
     * @return the weighted distance between x and y, in each of the dimensions
     */
    def distance(x: GroupedGroupFeature, y: GroupedGroupFeature): Double = {
      val d = similarityFunction(x.centroidSchema, y.centroidSchema)

      (x, y) match {
        case (_: BucketWindowGroupFeature[Any], _: BucketWindowGroupFeature[Any]) => throw new UnsupportedOperationException()
        case (x: BucketWindowGroupFeature[Any], _: GroupedGroupFeature) => x.N * d
        case (_: GroupedGroupFeature, y: BucketWindowGroupFeature[Any]) => y.N * d
        case _ => throw new UnsupportedOperationException()
      }
    }

    /**
     *
     * @param ps a set of the elements in the vector space
     * @return the centroid, as the union of the given data
     */
    def centroid(ps: Seq[GroupedGroupFeature]): GroupedGroupFeature = GroupedGroupFeatureAggregator.center(ps)
  }

}
