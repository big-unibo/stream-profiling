package it.unibo.big.utils

/** Keep track of elapsed time. */
object Timer {
  private var startTime = 0L
  private var time = 0L

  /** Start the timer. */
  def start(): Unit = {
    startTime = System.currentTimeMillis()
    time = startTime
  }

  /** @return elapsed time */
  def getElapsedTime: Long = System.currentTimeMillis() - startTime

}
