package it.unibo.big.streamprofiling.algorithm.utils

private[algorithm] object MapUtils {

  /**
   *
   * @param x             first sequence
   * @param y             second sequence
   * @param elementWiseOp the element wise function
   * @return the element-wise operation between the two arrays
   */
  private def elementWiseOperation(x: Map[String, Double], y: Map[String, Double], elementWiseOp: (Double, Double) => Double): Map[String, Double] = {
    var newMap = x
    y.foreach{
      case (v, d) => newMap += v -> elementWiseOp(newMap.getOrElse(v, 0D), d)
    }
    newMap
  }

  /**
   *
   * @param x first double map
   * @param y second double map
   * @return the element-wise sum of the sequences
   */
  def sumDouble(x: Map[String, Double], y: Map[String, Double]): Map[String, Double] = elementWiseOperation(x, y, (x, y) => x + y)

  /**
   *
   * @param x the input sequence
   * @param n the divisor of the element wise division
   * @return the element-wise division
   */
  def divide(x: Map[String, Double], n: Int): Map[String, Double] = elementWiseOperation(x, e => e / n)

  /**
   *
   * @param x             the input sequence
   * @param elementWiseOp element wise operation for x elements
   * @return the element-wise operation on the sequences
   */
  private def elementWiseOperation(x: Map[String, Double], elementWiseOp: Double => Double): Map[String, Double] = x.map{
    case (v, d) => v -> elementWiseOp(d)
  }

  /**
   *
   * @param x the input map
   * @return the element-wise square sequence
   */
  def square(x: Map[String, Double]): Map[String, Double] = elementWiseOperation(x, d => math.pow(d, 2))
}
