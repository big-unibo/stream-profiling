package it.unibo.big.streamprofiling.algorithm.execution

/**
 * Utility class for executing cluster algorithms in a window
 */
object ClusteringExecution {

  import ClusteringAlgorithms.ClusteringAlgorithm
  import it.unibo.big.streamprofiling.algorithm.Metrics._
  import it.unibo.big.streamprofiling.algorithm.implementation.incremental.twophases.Debug2PhasesAlgorithmsUtils
  import it.unibo.big.streamprofiling.algorithm.implementation.kmeans.KMeansOnSchemas.jaccardSpace
  import it.unibo.big.streamprofiling.algorithm.implementation.kmeans.KMeansWithoutK.computeClusteringInARange
  import it.unibo.big.streamprofiling.algorithm.utils.ClusteringExtractor.clusteringExtractorSchema
  import it.unibo.big.streamprofiling.algorithm.utils.DebugWriter
  import it.unibo.big.streamprofiling.algorithm.utils.JsonUtils.clustersWriter
  import it.unibo.big.utils.FileReader.MyConverter
  import it.unibo.big.utils.input.SchemaModeling.{ClusteringResult, Schema, TimestampedSchema, Window}
  import it.unibo.big.utils.input.window.DataWindow.windowing
  import it.unibo.big.utils.{FileReader, Timer}
  import org.json4s.jackson.JsonMethods.{compact, render}
  import org.slf4j.{Logger, LoggerFactory}

  import java.sql.Timestamp

  private val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)
  /**
   * Aggregates data in the window, using the given aggregateFunction.
   * @param windowDuration the window duration
   * @param slideDuration the slide duration
   * @param fileName      the simulation file name
   * @param clustering   the used clustering method
   * @param aggregateFunction the clustering aggregate function for a given window for a k specific value
   * @param bestKMetric       chosen metric for best k choose (default Silhouette)
   * @param dataFrequency     the input frequency of the data (from producer)
   * @param numberOfSeeds     the number of input seeds (from producer)
   * @param kMax              a function that given a number of records return the maximum value of k to try starting from 2
   * @param readAsAStream  if true read the data as a stream considering it already sorted
   * @param debugResult       if true tells to collect clustering result statistics (default false)
   * @param preAggregateFunction a function that pre-aggregates the result (for hierarchical clustering), for each k returns a result
   * @param startTime            if present is the simulation start time, otherwise start from the first instance in the file
   * @param endTime              if present is the simulation end time, otherwise end with the last instance in the file
   * @param elbowSensitivity     the sensitivity for elbow method, needed when the metric used is the SSE
   */
  def aggregateKRangeWindow(windowDuration: Long, slideDuration: Long, fileName: String, clustering: ClusteringAlgorithm,
                            aggregateFunction: (Seq[Schema], Int) => ClusteringResult,
                            bestKMetric: EvaluationMetric = Silhouette, dataFrequency: Long, numberOfSeeds: Long, kMax: Int => Int,
                            readAsAStream: Boolean, debugResult: Boolean = false, preAggregateFunction: Seq[Schema] => Map[Int, Seq[Seq[Schema]]],
                            startTime: Option[Long], endTime: Option[Long], randomInit: Boolean, elbowSensitivity: Option[Double]): Unit = {
    LOGGER.info("Start a computation")
    val schemas = readData(fileName, readAsAStream)
    windowing[TimestampedSchema](schemas, windowDuration, slideDuration, (window, data, _) => {
      val valueSeq = data.windowRecords
      LOGGER.info(s"Window $window (t = ${window.start.getTime}) with ${valueSeq.size} values")
      val inputRecords = if(!randomInit) valueSeq.sortBy(clusteringExtractorSchema.ordering) else valueSeq
      val windowResult = computeKRangeWindow(windowDuration, slideDuration, window.start.getTime, clustering, aggregateFunction, window, inputRecords, bestKMetric = bestKMetric,
        dataFrequency = dataFrequency, numberOfSeeds = numberOfSeeds, kMax = kMax, debugResult = debugResult, preAggregateFunction = preAggregateFunction, elbowSensitivity = elbowSensitivity)
      if(windowResult.nonEmpty) {
        LOGGER.info(windowResult.head)
      }
    }, startTime, endTime)
    LOGGER.info(s"End computation")
  }

  /**
   * Read the data from the file.
   * @param fileName the file name
   * @param readAsAStream if true lazy reading of the file considering it already sorted by time and value
   * @return the iterator of the file
   */
  def readData(fileName: String, readAsAStream: Boolean): Iterator[TimestampedSchema] = {
    if(readAsAStream) FileReader.readLazyFile(fileName, isResource = false)
      else FileReader.readFile(fileName, isResource = false).sortBy(x => (x.timestamp.getTime, x.value)).toIterator
  }

  implicit def simulationReader: MyConverter[TimestampedSchema] = new MyConverter[TimestampedSchema] {
    override def parse(line: String): TimestampedSchema = {
      val splitRegex = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"
      val Array(timestamp, seedName, value) = line.split(splitRegex).map(_.trim)
      val values = value.replace("[", "").replace("]", "").replace(" ", "")
        .replace("\"", "").split(splitRegex).toSet
      TimestampedSchema(values, new Timestamp(timestamp.toLong), seedName)
    }
  }

  /**
   * Aggregates data in the window, applying the aggregateFunction and trying various k values specified in the conf:
   * from "clustering.k-min" to "clustering.k-max".
   * Under the hood various statics files are updated regarding:
   *  1. input data distribution
   *  2. clustering results and timing.
   *
   * @param windowLength               window length
   * @param windowPeriod               window period
   * @param t                          the computed window time
   * @param clustering                 the used clustering method
   * @param aggregateFunction          the clustering aggregate function for the window for a k specific value
   * @param window                     the used window
   * @param kMax                       a function that given a number of records return the maximum value of k to try starting from 2
   * @param records                    the input window values
   * @param snapshotFileNameFirstRound the option window snapshot input file and a boolean value that tell if it is the first executed algorithm
   * @param bestKMetric                chosen metric for best k choose (default silhouette)
   * @param dataFrequency              the input frequency of the data (from producer)
   * @param numberOfSeeds              the number of input seeds (from producer)
   * @param debugResult                if true tells to collect clustering result statistics (default false)
   * @param preAggregateFunction       a function that pre-aggregates the result (for hierarchical clustering), for each k returns a result
   * @param elbowSensitivity           the sensitivity for elbow method, needed when the metric used is the SSE
   * @return the map of results for each k
   */
  private def computeKRangeWindow(windowLength: Long, windowPeriod: Long, t: Long, clustering: ClusteringAlgorithm, aggregateFunction: (Seq[Schema], Int) => ClusteringResult,
                                  window: Window, records: Seq[TimestampedSchema], snapshotFileNameFirstRound: Option[(String, Boolean)] = None,
                                  bestKMetric: EvaluationMetric = Silhouette, dataFrequency: Long, numberOfSeeds: Long, kMax: Int => Int, debugResult: Boolean = false,
                                  preAggregateFunction: Seq[Schema] => Map[Int, Seq[Seq[Schema]]], elbowSensitivity: Option[Double]
                   ): Seq[String] = {
    val startWindowComputationTime = System.currentTimeMillis()
    LOGGER.debug("Start compute pre-aggregate result")
    Timer.start()
    val precomputedResult = preAggregateFunction(records)
    LOGGER.debug(s"End compute pre-aggregate result ${Timer.getElapsedTime}")

    val (results, bestK) = computeClusteringInARange[Schema](windowLength, windowPeriod, numberOfSeeds, dataFrequency,
      2 to kMax(records.size), t, window, records, clustering,
      aggregateFunction, bestKMetric, precomputedResult, jaccardSpace, elbowSensitivity = elbowSensitivity)

    //total time for compute a window
    val totalTime = System.currentTimeMillis() - startWindowComputationTime
    if (results.nonEmpty) {
      val bestKResultStats = if (debugResult) results(bestK)._2.compute(Seq(SSE, Silhouette, AMI, ARI).filter(_ != bestKMetric)) else results(bestK)._2
      LOGGER.info(s"Best k = $bestK, $bestKMetric = ${bestKResultStats.metricsValues(bestKMetric)}, " +
        s"time = $totalTime, clusters: \n${
          results(bestK)._1.zipWithIndex.map {
            case ((schema, xs), i) => i -> s"c${i + 1}: ${schema.value} #elem: ${xs.size}, avg(dist): ${xs.map(_._2).sum / xs.size}"
          }.toSeq.sortBy(_._1).map(_._2).mkString("\n")
        }")
      //write frequency statistics
      DebugWriter.writeFrequencyStatistics(bestKResultStats, totalTime, 0L, usedMetric = Some(bestKMetric))
      Debug2PhasesAlgorithmsUtils.writeClustersStatisticsForClusteringResult(t, window, results(bestK)._1, clustering.toString)
      if(debugResult) {
        results.map {
          case (k, (result, _)) => compact(render(clustersWriter(clustering.toString, k).write(result)))
        }.toSeq
      } else {
        //write just the best clustering result
        Seq(compact(render(clustersWriter(clustering.toString, bestK).write(results(bestK)._1))))
      }
    } else {
      Seq()
    }
  }
}
