package it.unibo.big.streamprofiling.algorithm.implementation.incremental.twophases.CSCS

import scala.util.Random

/**
 *
 * @param dictionary data dictionary
 * @param w the tolerance of the hash function
 * @param index index of hash function to use as seed
 */
private [CSCS] class EuclideanHash(dictionary: IndexedSeq[String], w: Double, index: Int) {
  private def nextDoubleExclusive(origin: Double, bound: Double, rnd: Random): Double = {
    val DOUBLE_UNIT = 1.0 / (1L << 53)
    var r = (rnd.nextLong >>> 11) * DOUBLE_UNIT
    if (origin < bound) {
      r = r * (bound - origin) + origin
      if (r >= bound) { // correct for rounding
        r = java.lang.Double.longBitsToDouble(java.lang.Double.doubleToLongBits(bound) - 1)
      }
    }
    r
  }

  require(w > 0)
  import it.unibo.big.streamprofiling.algorithm.utils.RandomUtils.RANDOM_SEED
  import it.unibo.big.utils.input.SchemaModeling.Schema

  private val dimension = dictionary.size

  private val rnd = new Random
  rnd.setSeed(RANDOM_SEED + index)

  private val b = nextDoubleExclusive(0, w, rnd) //a random in [0,w) 0 <= x < b
  private val a : IndexedSeq[Double] = (0 until dimension).map(_ => rnd.nextGaussian())

  /**
   *
   * @param s schema to hash
   * @return the hash value of the schema
   */
  def hash(s: Schema): Int = {
    require(s.values.keySet.forall(dictionary.contains))
    val x = dictionary.map(x => s.values.getOrElse(x, 0D))
    val aDotx = a.indices.map(i => a(i) * x(i)).sum
    ((aDotx + b)/w).floor.toInt // floor
  }
}