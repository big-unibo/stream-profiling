package it.unibo.big.streamprofiling.algorithm.utils

/**
 * Utilities for Random data
 */
private [streamprofiling] object RandomUtils {
  import scala.util.Random

  /**
   * Random seed, for reproducibility
   */
  val RANDOM_SEED = 10L

  /**
   *
   * @param xs an input sequence, need to be ordered for reproducibility
   * @param k  the number of elements to pick
   * @param random boolean parameter to indicate to not use or use random seed
   * @param randomSeedOffset an integer that indicate if add an offset to the random seed
   * @tparam T the type of the elements
   * @return the k picked random values
   */
  def pickRandom[T](xs: Seq[T], k: Int, random: Boolean, randomSeedOffset: Int = 0): Seq[T] = {
    if(!random) {
      Random.setSeed(RANDOM_SEED + randomSeedOffset)
    }
    Random.shuffle(xs).take(k)
  }
}
