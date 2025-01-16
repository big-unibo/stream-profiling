package it.unibo.big.streamprofiling.algorithm.utils

import scala.language.postfixOps

object DebugWriter {

  import ClusteringExtractor._
  import MonitoringProperty.MonitoringProperty
  import SchemaUtils.DistanceMap
  import Settings.CONFIG
  import VectorSpace.VectorSpace
  import it.unibo.big.streamprofiling.algorithm.Metrics._
  import it.unibo.big.streamprofiling.algorithm.execution.ClusteringAlgorithms.ClusteringAlgorithm
  import it.unibo.big.utils.input.SchemaModeling.{TimestampedSchema, Window}
  import it.unibo.big.utils.{FileWriter, Timer}
  import org.slf4j.{Logger, LoggerFactory}

  private val LOGGER : Logger = LoggerFactory.getLogger(getClass.getName)

  private val TIME_HEADER: Seq[String] = Seq("t", "window_start", "window_end", "window_period", "k", "cluster-type", "clustering-parameters",
    "records", "elapsed-time", "sse", "silhouette", "AMI", "ARI", "seeds", "data_frequency", "windowLength", "windowPeriod")

    /**
   * Write statistics about stress tests on "window.frequency_statistics" file
   * @param timeStatistics timeStatistic
   * @param totalTime Total time for get the best result
   * @param timeShapeResult Total time for get the best result (included in total time)
   * @param additionalStatistics additional statistics of clustering
   * @param usedMetric the used metric for choose the best result, if not present none
   */
  def writeFrequencyStatistics[T, C](timeStatistics: WindowTimeStatistics[T, C], totalTime: Long, timeShapeResult: Long, additionalStatistics: Map[MonitoringProperty, Any] = Map(), usedMetric: Option[Metric] = None): Unit = {
    val sortedMonitoringValues = MonitoringProperty.values.toSeq.sortBy(_.toString)
    FileWriter.writeFileWithHeader(Seq(timeStatistics.toSeq ++ Seq(totalTime, timeShapeResult, usedMetric.orNull) ++ sortedMonitoringValues.map(p => additionalStatistics.getOrElse(p, null))),
      TIME_HEADER ++ "totalTime, timeForShapeResult, metric" .split(""", """) ++ sortedMonitoringValues.map(_.toString),
      CONFIG.getString("window.frequency_statistics"))
  }

    /**
   * A case class for write statics about results of window computations
   *
   * @param t the computed window starting time of the window in milliseconds
   * @param window the used window
   * @param additionalInfo parameter of clustering algorithm
   * @param clustering the applied clustering method
   * @param numberOfRecords the number of input records for the computation
   * @param time the computed time for compute the window
   * @param result the clustering result
   * @param metrics the metrics want to compute for the statics
   * @param vectorSpace the used vector space (jaccard space by default)
   * @param windowLength duration of the window
   * @param windowPeriod period of the window
   */
  case class WindowTimeStatistics[T, C](t: Long, window: Window, additionalInfo: T, clustering: ClusteringAlgorithm, numberOfRecords: Int,
                                        time: Long, result: Map[C, Seq[(C, Double)]], metrics: Seq[Metric],
                                        numberOfSeeds: Long, dataFrequency: Long, vectorSpace: VectorSpace[C], windowLength: Long, windowPeriod: Long)(implicit clusteringExtractor: ClusteringExtractor[C]) {
    /**
     * computes other metrics.
     * @param metrics metrics to compute
     * @return this, updated
     */
    def compute(metrics: Seq[Metric]): WindowTimeStatistics[T, C] = {
      LOGGER.debug(s"Start compute metrics extra metrics for best result -- $metrics"); Timer.start()
      metricsValuesVar = metricsValuesVar ++ metrics.map(m => m -> m.compute(result, vectorSpace)).toMap
      LOGGER.debug(s"Metrics computation took ${Timer.getElapsedTime}")
      this
    }

    LOGGER.debug(s"Start compute metrics $metrics"); Timer.start()
    private var metricsValuesVar: Map[Metric, Double] = metrics.map(m => m -> m.compute(result, vectorSpace)).toMap
    LOGGER.debug(s"Metrics computation took ${Timer.getElapsedTime}")

    /**
     * @return the metrics computed
     */
    def metricsValues: Map[Metric, Double] = metricsValuesVar

    /**
     *
     * @return Sequence representation of window time statistics
     */
    def toSeq: Seq[Any] =  Seq(t, window.start.getTime, window.end.getTime, window.end.getTime - window.start.getTime, additionalInfo,
      clustering.toString, clustering.parameters, numberOfRecords, time, metricsValues.getOrElse(SSE, null),
      metricsValues.getOrElse(Silhouette, null), metricsValues.getOrElse(AMI, null), metricsValues.getOrElse(ARI, null), numberOfSeeds, dataFrequency, windowLength, windowPeriod)
  }
}
