package it.unibo.big.streamprofiling.algorithm.implementation.incremental.feacstream

import it.unibo.big.streamprofiling.algorithm.implementation.incremental.IncrementalClustering
import it.unibo.big.streamprofiling.algorithm.utils.{MonitoringProperty, MonitoringStats}
import it.unibo.big.utils.input.SchemaModeling.Schema

/**
 * FEAC-Stream implementation as Incremental algorithm
 *
 * "An evolutionary algorithm for clustering data streams with a variable number of clusters"
 *
 * @param initSize size for start clustering, option if none take the first window
 * @param generateNumberOfPopulations number of populations for FEAC algorithm, indicate how replicate copies generate for a population (not used in first FEAC initialization)
 * @param kmeansIterations number of k-means iterations in FEAC
 * @param elementsForMutation number of elements to apply mutation, is a function for FEAC that varies w.r.t. the number of individual
 * @param startingK starting number of clusters (k), optional -- for test purpose
 * @param maxK maximum number of clusters (k), optional -- for test purpose
 * @param saveRawData save raw data of window elements
 * @param randomInit random initialization of FEAC
 */
private[algorithm] class FeacStreamAlgorithm(initSize: Option[Int], generateNumberOfPopulations: Int,
                                             kmeansIterations: Int, elementsForMutation : Int => Int, startingK: Option[Int], maxK: Option[Int], saveRawData: Boolean, randomInit: Boolean) extends IncrementalClustering[Schema] {
  import FeacStreamAlgorithmDetails._
  import PageHinkley.PageHinkleyTest
  import it.unibo.big.streamprofiling.algorithm.implementation.kmeans.KMeansOnSchemas.jaccardSpace
  import it.unibo.big.streamprofiling.algorithm.utils.VectorSpace.VectorSpace
  import it.unibo.big.utils.input.SchemaModeling
  import it.unibo.big.utils.input.SchemaModeling._
  import org.slf4j.{Logger, LoggerFactory}

  private var clustering : FeacStreamClusteringResult = FeacStreamClusteringResult()
  private var buffer: Buffer = new Buffer()
  private val phTester = new PageHinkleyTest()
  private val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)
  private var initSizeValue : Option[Int] = None
  private var monitoringStats: MonitoringStats = MonitoringStats()

  override def updateResult(t: Long, window: SchemaModeling.Window, newSchemas: Seq[SchemaWithTimestamp], oldestSchemas: Seq[SchemaWithTimestamp]): Unit = {
    monitoringStats = MonitoringStats()
    LOGGER.info(s"Start insert data in the result, max k = $maxK startK = $startingK")
    if(initSizeValue.isEmpty) {
      initSizeValue = Some(if(initSize.nonEmpty) initSize.get else newSchemas.size)
    }
    for(e <- newSchemas) {
      val r = insertion(clustering, e, window.paneStart(e).get, initSizeValue.get, phTester, buffer, generateNumberOfPopulations, kmeansIterations, elementsForMutation, window, startingK, maxK, saveRawData, randomInit, monitoringStats)
      clustering = r._1
      buffer = r._2
      monitoringStats = r._3
    }
    //move deletion above new insertions
    LOGGER.info("End insert data in the result")
    LOGGER.info("Remove data from old result")
    val time = System.currentTimeMillis()
    clustering = removeOldestClusteringResult(clustering, window, saveRawData)
    monitoringStats = monitoringStats.updateValue(MonitoringProperty.TIME_FOR_REMOVE_OLD_RECORDS, System.currentTimeMillis() - time)
    LOGGER.info("End remove data from old result")
  }

  override def actualClusteringResult(window: Window): ClusteringResult =  {
    clustering.schemas.filterKeys(window.contains).flatMap{
      case (t, sx) if window.contains(t) =>
        sx.indices.map(i => sx(i).asInstanceOf[Schema] -> clustering.encodingScheme(t)(i))
    }.groupBy(_._2).map{
      case (c, tuples) =>
        val centroid = clustering.clusters(c).centroidSchema
        centroid -> tuples.keys.map(t => t -> vectorSpace.distance(t, centroid)).toSeq
    }
  }

  override def vectorSpace: VectorSpace[Schema] = jaccardSpace

  override def statistics: MonitoringStats = monitoringStats
}
