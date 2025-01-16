package it.unibo.big.streamprofiling.algorithm.utils

object ClusteringExtractor {
  import it.unibo.big.utils.input.SchemaModeling.Schema

  /**
   * Trait for extract data for clustering purpose.
   *
   * @tparam A type of data to extract
   */
  trait ClusteringExtractor[A] {
    /**
     *
     * @param x an element in the extractor
     * @return a function for ordering the element
     */
    def ordering(x: A): String

    /**
     *
     * @param x an element in the extractor
     * @return the class of a
     */
    def className(x: A): String

    /**
     *
     * @param x an element in the extractor
     * @return the center representation of the element, default identity
     */
    def toCenter(x: A): A = x
  }

  implicit val clusteringExtractorSchema: ClusteringExtractor[Schema] = new ClusteringExtractor[Schema] {
    override def ordering(x: Schema): String = x.value

    override def className(x: Schema): String = x.seedName
  }
}
