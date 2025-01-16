package it.unibo.big.streamprofiling.algorithm.implementation.kmeans

/**
 * Some simplifications for apply various version of kmeans in the specific context of schemas
 */
object KMeansOnSchemas {
  import KMeans.kmeans
  import it.unibo.big.streamprofiling.algorithm.utils.MapUtils._
  import it.unibo.big.streamprofiling.algorithm.utils.VectorSpace.VectorSpace
  import it.unibo.big.utils.input.SchemaModeling.{BasicSchema, ClusteringResult, Schema}

  private def schemaFuzzyCentroid(ps: Seq[Schema]): Schema = if (ps.isEmpty) BasicSchema(Set[String]()) else {
    BasicSchema(divide(ps.map(_.values).reduce(sumDouble), ps.size))
  }

  /**
   * The jaccard space of the kmeans
   */
  def jaccardSpace: VectorSpace[Schema] = new VectorSpace[Schema] {
    def distance(x: Schema, y: Schema): Double = x.jaccard(y)

    /**
     *
     * @param ps a set of the elements in the vector space
     * @return the centroid of the given set, that is the union of the values of all the schema
     */
    def centroid(ps: Seq[Schema]): Schema = schemaFuzzyCentroid(ps)
  }

  /**
   *
   * The Euclidean schema space, as a co-occurrence record of boolean
   */
  val euclideanSpace: VectorSpace[Schema] = new VectorSpace[Schema] {
    /**
     *
     * @param x an element in the vector space
     * @param y another element in the vector space
     * @return the euclidian between x and y, in each of the dimensions
     */
    def distance(x: Schema, y: Schema): Double = x.euclideanDistance(y)

    /**
     *
     * @param ps a set of the elements in the vector space
     *  @return the centroid, as the union of the given data
     */
    def centroid(ps: Seq[Schema]): Schema = schemaFuzzyCentroid(ps)
  }

  /**
   *
   * @param records the input records
   * @param k kmeans parameter
   * @param randomInit true if random init, false if kmeans++
   * @return the standard kmeans result on the jaccard space
   */
  def predictCentroid(records: Seq[Schema], k: Int, randomInit: Boolean): ClusteringResult = kmeans(records, k, jaccardSpace, randomInit)

  /**
   *
   * @param s input schemas
   * @return empty pre aggregate result
   */
  def centroidPreAggregateFunction(s: Seq[Schema]): Map[Int, Seq[Seq[Schema]]] = Map()
}
