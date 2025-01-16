package it.unibo.big.streamprofiling.algorithm.execution

import it.unibo.big.streamprofiling.algorithm.implementation.incremental.twophases.AbstractTwoPhasesClustering
import it.unibo.big.utils.input.window.DataWindow.WindowData

/**
 * Utility object for the execution of incremental clustering algorithms in a window
 */
object IncrementalClusteringExecution {
  import ClusteringAlgorithms.ClusteringAlgorithm
  import ClusteringExecution.readData
  import it.unibo.big.streamprofiling.algorithm.Metrics._
  import it.unibo.big.streamprofiling.algorithm.implementation.incremental.IncrementalClustering
  import it.unibo.big.streamprofiling.algorithm.implementation.incremental.twophases.CSCS.CoreSetBasedClusteringWithSlidingWindows
  import it.unibo.big.streamprofiling.algorithm.implementation.incremental.twophases.DSC.DynamicStreamClustering
  import it.unibo.big.streamprofiling.algorithm.implementation.incremental.twophases.Debug2PhasesAlgorithmsUtils
  import it.unibo.big.streamprofiling.algorithm.implementation.kmeans.KMeansOnSchemas.jaccardSpace
  import it.unibo.big.streamprofiling.algorithm.utils.DebugWriter
  import it.unibo.big.streamprofiling.algorithm.utils.DebugWriter.WindowTimeStatistics
  import it.unibo.big.streamprofiling.algorithm.utils.JsonUtils.clustersWriter
  import it.unibo.big.utils.input.SchemaModeling
  import it.unibo.big.utils.input.SchemaModeling.{ClusteringResult, Schema, SchemaWithTimestamp, TimestampedSchema}
  import it.unibo.big.utils.input.window.DataWindow.windowing
  import org.json4s.jackson.JsonMethods.{compact, render}
  import org.slf4j.{Logger, LoggerFactory}

  import java.sql.Timestamp

  private val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  /**
   * Auxiliary method for compute a sliding window of incremental clustering
   *
   * @param windowLength window length
   * @param windowPeriod     window period
   * @param frequency        frequency of data (from producer)
   * @param numberOfSeeds    number of seeds in generation (from producer)
   * @param windowData       the window data
   * @param clustering       the clustering algorithm
   * @param clusteringMethod the clustering method of the algorithm
   * @param window           the used window
   * @param windowTime       a time that identifies the window
   * @param debugResult      a flag that if is true indicates to debug the result
   * @return the clustering result and the correspondence centroids of the clusters considering previous window (if it is a two phases algorithm)
   */
  private def computeWindow[T, V <: SchemaWithTimestamp](windowLength: Long, windowPeriod: Long, frequency: Long, numberOfSeeds: Long, windowData: WindowData[V], clustering: IncrementalClustering[T],
                                                         clusteringMethod: ClusteringAlgorithm, window: SchemaModeling.Window,
                                                         windowTime: Long, debugResult: Boolean): (ClusteringResult, Map[Map[String, Double], Option[Map[String, Double]]]) = {
    val clusteringName = clusteringMethod.toString
    val startWindowComputationTime = System.currentTimeMillis()
    LOGGER.info(s"Window $window (t (slide) = ${new Timestamp(windowTime)}) with ${windowData.windowRecords.size} values (new data = ${windowData.newPaneRecords.size})")
    clustering.updateResult(if(clustering.isInstanceOf[CoreSetBasedClusteringWithSlidingWindows]) windowTime else window.start.getTime, window, windowData.newPaneRecords, windowData.oldPaneRecords)
    val startObtainingFullResultTime = System.currentTimeMillis()
    val clusteringResult = clustering.actualClusteringResult(window)
    val endTime = System.currentTimeMillis()
    val time = endTime - startWindowComputationTime
    val timeForShapeResult = endTime - startObtainingFullResultTime
    val result = compact(render(clustersWriter(clusteringName, clusteringResult.size).write(clusteringResult)))
    val stat = WindowTimeStatistics[Int, Schema](windowTime, window, clusteringResult.size, clusteringMethod, windowData.windowRecords.size,
      time, clusteringResult, if (debugResult) Seq(SSE, AMI, ARI, Silhouette) else Seq(SSE, Silhouette), numberOfSeeds, frequency, jaccardSpace, windowLength, windowPeriod)

    clustering match {
      case h: CoreSetBasedClusteringWithSlidingWindows =>
        //write statistics for lsh
        Debug2PhasesAlgorithmsUtils.writeClustersDistances(windowTime, window, h.groupedFeatureClusteringResult, clusteringName)
        Debug2PhasesAlgorithmsUtils.writeClustersStatistics(windowTime, window, h.groupedFeatureClusteringResult, clusteringName)
        Debug2PhasesAlgorithmsUtils.writeOperationTimes(h.getOperationsTimes, clusteringName)
        h match {
          case dsc: DynamicStreamClustering =>
            dsc.writeDSCStatistics(windowTime, window)
          case _ =>
        }
      case _ =>
    }
    DebugWriter.writeFrequencyStatistics(stat, time, timeForShapeResult, clustering.statistics.getAllValues)
    LOGGER.info(s"The result is of clusters ${clusteringResult.size} and $result")
    val clusteredRecords = clusteringResult.toSeq.map(_._2.size).sum
    LOGGER.info(s"Cluster of $clusteredRecords records (data = ${windowData.windowRecords.size})")
    //require(clusteredRecords == data.size)
    LOGGER.info(stat.metricsValues.mkString(", "))
    (clusteringResult, clustering match {
      case c if c.isInstanceOf[AbstractTwoPhasesClustering[T]] => c.asInstanceOf[AbstractTwoPhasesClustering[T]].getCorrespondenceCentroids
      case _ => Map.empty
    })
  }

  /**
   * Computation of incremental clustering, in a stored window
   *
   * @param windowDuration the window duration
   * @param slideDuration the slide duration
   * @param fileName       the file where the simulation of the producer is stored
   * @param clustering the used clustering algorithm implementation
   * @param clusteringMethod the clustering method of the algorithm
   * @param dataFrequency  the input frequency of the data (from producer)
   * @param numberOfSeeds  the number of input seeds (from producer)
   * @param readAsAStream  if true read the data as a stream considering it already sorted
   * @param debugResult    if true debug the quality of clustering results
   * @param startTime      if present is the simulation start time, otherwise start from the first instance in the file
   * @param endTime        if present is the simulation end time, otherwise end with the last instance in the file
   */
  def aggregateStoredWindow[T](windowDuration: Long, slideDuration: Long, fileName: String, dataFrequency: Long, numberOfSeeds: Long,
                               clustering: IncrementalClustering[T], clusteringMethod: ClusteringAlgorithm,
                               readAsAStream: Boolean, debugResult: Boolean = false, startTime: Option[Long], endTime: Option[Long]): Unit = {
    val schemas = readData(fileName, readAsAStream)
    windowing[TimestampedSchema](schemas, windowDuration, slideDuration, (window, data, windowTime) => {
      computeWindow(windowDuration, slideDuration, dataFrequency, numberOfSeeds, data, clustering, clusteringMethod, window, windowTime, debugResult)
    }, startTime, endTime)
  }
}
