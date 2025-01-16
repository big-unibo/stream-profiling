package it.unibo.big.streamprofiling.algorithm.utils

/**
 * Object for the trait of the VectorSpace, i.e. the space definition trait for kmeans
 */
object VectorSpace {
  trait VectorSpace[A] {
    /**
     *
     * @param x an element in the vector space
     * @param y another element in the vector space
     * @return the distance between x and y
     */
    def distance(x: A, y: A): Double

    /**
     *
     * @param ps a set of the elements in the vector space
     * @return the centroid of the given set
     */
    def centroid(ps: Seq[A]): A
  }
}
