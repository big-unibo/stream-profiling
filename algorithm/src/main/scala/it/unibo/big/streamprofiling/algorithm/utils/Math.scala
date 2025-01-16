package it.unibo.big.streamprofiling.algorithm.utils

private [streamprofiling] object Math {
  import Numeric.Implicits._

  private def mean[T: Numeric](xs: Iterable[T]): Double = xs.sum.toDouble / xs.size

  def median(seq: Seq[Double]): Double = {
    val sortedSeq = seq.sorted
    if (seq.size % 2 == 1) sortedSeq(sortedSeq.size / 2) else if(seq.nonEmpty) {
      val (up, down) = sortedSeq.splitAt(seq.size / 2)
      (up.last + down.head) / 2
    } else 0D
  }

  /**
   *
   * @param xs a seq of double
   * @return the MAD with the scaling factor
   */
  def mad(xs: Seq[Double]): Double = {
    val medianXs = median(xs)
    median(xs.map(x => (x - medianXs).abs))
  }

  def variance[T: Numeric](xs: Iterable[T]): Double = {
    val avg = mean(xs)

    xs.map(_.toDouble).map(a => math.pow(a - avg, 2)).sum / xs.size
  }

  /**
   *
   * @param value the actual value
   * @param median the median in the dataset
   * @param mad the mad in the dataset
   * @return the robust zscore of the value, if nan returns 0
   */
  def robustZScore(value: Double, median: Double, mad: Double): Double = {
    val epsilon = 1e-2
    val score = (value - median) / (1.4826 * mad + epsilon) //multiply for scale factor for MAD for non-normal distribution
    if(score.isNaN) 0 else score
  }
}
