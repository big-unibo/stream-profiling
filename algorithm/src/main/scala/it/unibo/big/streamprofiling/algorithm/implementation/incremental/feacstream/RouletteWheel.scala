package it.unibo.big.streamprofiling.algorithm.implementation.incremental.feacstream

/**
 * Object for the Roulette Wheel extraction in FEAC Algorithm
 */
private [feacstream] object RouletteWheel {
  import it.unibo.big.streamprofiling.algorithm.utils.RandomUtils.RANDOM_SEED

  import scala.util.Random
  /**
   *
   * @param input a sequence fo ranked data
   * @tparam T type of sequence
   * @param random boolean parameter to indicate to use random seed or not
   * @param randomOffset an integer that indicate if add an offset to the random seed
   * @return the stream for the extraction
   */
  private def weightedSelect[T](random: Boolean, randomOffset: Int, input: (T, Int)*): Seq[T] = {
    val items: Seq[T] = input.flatMap { x => Seq.fill(x._2)(x._1) }

    def output: Seq[T] = {
      if(!random) {
        Random.setSeed(RANDOM_SEED + randomOffset)
      }
      Random.shuffle(items)
    }

    output
  }

  /**
   *
   * @param input the input sequence
   * @param n number of elements to pick
   * @param random boolean parameter to indicate to use random seed or not
   * @param randomOffset an integer that indicate if add an offset to the random seed
   * @tparam T the value type
   * @return the picked results seq
   */
  private def pickWeighted[T](random: Boolean, input: Seq[(T, Int)], n: Int, randomOffset: Int): Seq[T] = weightedSelect(random, randomOffset, input :_*).take(n)

  /**
   *
   * @param input the input sequence with a value
   * @param n number of elements to pick
   * @param ordering the ordering of the value
   * @param random boolean parameter to indicate to use random seed or not
   * @param randomOffset an integer that indicate if add an offset to the random seed
   * @tparam T the value type
   * @tparam V the ordering value type
   * @return the picked results seq
   */
  def pickWeighted[T, V](input: Seq[(T, V)], n: Int, ordering: Ordering[V], random: Boolean, randomOffset: Int = 0): Seq[T] = {
    var taken = Seq[T]()
    (0 until n).foreach { _ =>
      val inputSorted = input.filter(x => !taken.contains(x._1)).sortBy(_._2)(ordering).zipWithIndex
      val overallSum = inputSorted.size //  inputSorted.indices.sum + inputSorted.size
      val inputWeighted = inputSorted.map {
        case ((v, _), i) => v -> (((i + 1).toDouble / overallSum) * 100).floor.toInt
      }
      taken ++= pickWeighted(random, inputWeighted, 1, randomOffset)
    }
    taken
  }
}
