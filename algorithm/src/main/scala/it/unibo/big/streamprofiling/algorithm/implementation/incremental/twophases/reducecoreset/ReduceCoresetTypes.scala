package it.unibo.big.streamprofiling.algorithm.implementation.incremental.twophases.reducecoreset

import it.unibo.big.streamprofiling.algorithm.implementation.incremental.groupfeature.BucketWindowGroupFeature

/**
 * Object for different definition of reduce coreset
 */
object ReduceCoresetTypes {

  /**
   * Trait for different implementations of reduce coreset
   */
  sealed trait ReduceCoresetType

  /**
   * The original definition of merge corset in the buckets with a distance function
   *
   * @tparam T types of hash values in the bucket
   */
  sealed trait StandardReducing[T] extends ReduceCoresetType {
    /**
     *
     * @param bucket1 a bucket
     * @param bucket2 a second bucket
     * @return the similarity between bucket1 and bucket2
     */
    def distanceFunction(bucket1: BucketWindowGroupFeature[T], bucket2: BucketWindowGroupFeature[T]): Double
  }

  /**
   * Distance function of hash values is distance from centroids GF
   */
  case object CENTROID extends StandardReducing[Any] {
    override def distanceFunction(bucket1: BucketWindowGroupFeature[Any], bucket2: BucketWindowGroupFeature[Any]): Double = bucket1.centroidSchema.jaccard(bucket2.centroidSchema)
  }

  /**
   * Distance function of hash values is distance from centroids GF
   */
  private case object CENTROIDEU extends StandardReducing[Any] {
    override def distanceFunction(bucket1: BucketWindowGroupFeature[Any], bucket2: BucketWindowGroupFeature[Any]): Double = bucket1.centroidSchema.euclideanDistance(bucket2.centroidSchema)
  }

  /**
   * Distance function of hash values is manhattan
   */
  case object STANDARD extends StandardReducing[Int] {
    override def distanceFunction(bucket1: BucketWindowGroupFeature[Int], bucket2: BucketWindowGroupFeature[Int]): Double = bucket1.bucket.zip(bucket2.bucket).map(v => (v._1 - v._2).abs).sum
  }

  /**
   * Distance function of hash values is hamming
   */
  private case object HAMMING extends StandardReducing[Int] {
    override def distanceFunction(bucket1: BucketWindowGroupFeature[Int], bucket2: BucketWindowGroupFeature[Int]): Double = {
      bucket1.bucket.zip(bucket2.bucket).map(v => if(v._1 == v._2) 0 else 1).sum
    }
  }

  /**
   * Distance function of hash values is jaccard 1 - (|A intersect B| / |A union B|)
   */
  private case object JACCARD extends StandardReducing[Int] {
    override def distanceFunction(bucket1: BucketWindowGroupFeature[Int], bucket2: BucketWindowGroupFeature[Int]): Double = {
      1 - bucket1.bucket.intersect(bucket2.bucket).size.toDouble / bucket1.bucket.union(bucket2.bucket).size
    }
  }

  /**
   * Uses an alternative method to merge similar buckets, based on slicing the hash values, using a low number of hash function
   */
  case object HASH_LENGTH extends ReduceCoresetType

  /**
   *
   * @param s string to parse
   * @return the ReduceCoresetType from the string, STANDARD if not found
   */
  def fromString(s: String): ReduceCoresetType = Seq(STANDARD, CENTROIDEU, CENTROID, HASH_LENGTH, HAMMING, JACCARD).find(_.toString == s).getOrElse(STANDARD)
}
