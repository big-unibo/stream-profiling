package it.unibo.big.streamprofiling.algorithm

/**
 * Object with methods for compute clustering metrics
 */
object Metrics {

  import com.typesafe.config.ConfigFactory
  import it.unibo.big.streamprofiling.algorithm.implementation.incremental.groupfeature.GroupedGroupFeature
  import it.unibo.big.streamprofiling.algorithm.utils.ClusteringExtractor._
  import it.unibo.big.streamprofiling.algorithm.utils.VectorSpace.VectorSpace
  import org.slf4j.{Logger, LoggerFactory}

  import java.io.{BufferedReader, InputStreamReader}

  val DEFAULT_INDEX: Int = -1
  private val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)
  //python raw is set by algorithm api
  private val pythonRaw: Boolean = util.Try(ConfigFactory.parseResources("application.conf").getBoolean("native_python")).toOption.getOrElse(false)
  private val windows = sys.props("os.name").toLowerCase.contains("win")
  private val pythonInterpreter = if(pythonRaw) if(windows) "python" else "python3" else s"""myenv/${if (windows) "Scripts/python.exe" else "bin/python"}"""

  /**
   * Enumeration for the used metrics
   */
  sealed trait Metric {

    /**
     * @param result      the clustering result
     * @param vectorSpace the used vector space. Used for the silhouette computation
     * @tparam T          the data type of clustered data
     * @return the double result of the metric computation on the clustering result
     */
    def compute[T: ClusteringExtractor](result: Map[T, Seq[(T, Double)]], vectorSpace: VectorSpace[T]): Double = this match {
      case SSE => sse(result)
      case Silhouette => simplifiedSilhouette(result, vectorSpace)._2
      case AMI => ami(result)(implicitly[ClusteringExtractor[T]])
      case ARI => ari(result)(implicitly[ClusteringExtractor[T]])
    }

    /**
     *
     * @param actualValue actual value
     * @param oldValue old value
     * @return true if the actual value is better than the old one, w.r.t to the metric
     */
    def compare(actualValue: Double, oldValue: Double): Boolean = this match {
      case SSE => actualValue <= oldValue
      case AMI | Silhouette | ARI => actualValue >= oldValue
    }
  }

  /**
   * Trait for the metrics that are used for evaluate the best result of a set of clustering results
   */
  sealed trait EvaluationMetric extends Metric

  case object SSE extends EvaluationMetric {
    /**
     *
     * @param results the clustering results, with sse
     * @return the best k using the elbow method
     */
    def chooseBestResult[T](results: Map[Int, (Map[T, Seq[(T, Double)]], Double)], elbowSensitivity: Double): Int = {
      val resultSeq = results.toSeq.map(x => (x._1, x._2)).sortBy(_._1)
      val x = resultSeq.map(_._1)
      val y = resultSeq.map(_._2._2)

      val xString = "\"" + s"[${x.mkString(",")}]" + "\""
      val yString = "\"" + s"[${y.mkString(",")}]" + "\""
      val pythonScript = "tmp/elbow.py"
      val command = Seq(pythonInterpreter, pythonScript, elbowSensitivity.toString)

      import it.unibo.big.utils.FileWriter.substituteConfigurationAndWriteNewFile

      substituteConfigurationAndWriteNewFile("algorithm/src/main/python/it/big/unibo/streamprofiling/algorithm/kmeans/elbowpy",
        isResource = false,
        "tmp/elbow.py", Map(
          "elbow.x" -> xString,
          "elbow.y" -> yString
        )
      )

      val processBuilder = new ProcessBuilder(command : _*)
      val process = processBuilder.start()
      val exitCode = process.waitFor()
      val output = scala.io.Source.fromInputStream(process.getInputStream).getLines.mkString
      LOGGER.info(s"Python script exit code: $exitCode")
      LOGGER.info(s"Python script output: $output")

      if (exitCode == 0) {
        output.replace("\r\n", "").toInt
      } else {
        val errorStream = new BufferedReader(new InputStreamReader(process.getErrorStream))
        val errorMessage = Stream.continually(errorStream.readLine()).takeWhile(_ != null).mkString(System.lineSeparator())
        require(exitCode == 0, s"Error in python script execution $errorMessage, try to create a venv (myvenv)")
        DEFAULT_INDEX
      }
    }
  }

  /**
   * Silhouette metric
   */
  case object Silhouette extends EvaluationMetric {
    /**
     *
     * @param kSilhouette    actual value of silhouette
     * @param lastSilhouette last available silhouette of previous k
     * @param k              actual k value
     * @param recordsSize    the number of involved records
     * @return
     *  - an int that defines the bestK at the moment, if the silhouette value now is less than the last one the best k is k-1, otherwise is k
     *  - a double that is referred to the bestK computation
     *  - a boolean that tells to continue to compute k + 1 or stop, i.e. tells if the best k have been found
     */
    def chooseBestResult(kSilhouette: Double, lastSilhouette: Option[(Double, Int)], k: Int, recordsSize: Int): (Int, Double, Boolean) = {
      val compute = k <= recordsSize
      (k, kSilhouette, compute) //Remove peak local search in silhouette
    }

    /**
     *
     * @param results the clustering results, with silhouette
     * @tparam T the type of clustered data
     * @return best k with maximum silhouette, using minimum k available
     */
    def chooseBestResult[T](results: Map[Int, (Map[T, Seq[(T, Double)]], Double)]): Int = {
      val maxSilhouette = results.map(_._2._2).max
      results.filter(_._2._2 == maxSilhouette).minBy(_._1)._1
    }
  }

  case object AMI extends Metric
  case object ARI extends Metric

  /**
   *
   * @param result the result of a clustering computation
   * @return the SSE on the result as the squared sum of the distances from the cluster centroids
   */
  private def sse[T](result: Map[T, Seq[(T, Double)]]): Double = result.flatMap(_._2).flatMap{
    case (c: GroupedGroupFeature, x) => Seq.fill(c.N)(math.pow(x / c.N, 2))
    case (_, x) => Seq(x * x)
  }.sum

  type SSResult[T] = (Map[T, Double], Double)
  /**
   *
   * @param result the clustering result
   * @param vectorSpace vector space
   * @return the simplified silhouette, both for clusters and of the solution
   */
  def simplifiedSilhouette[T](result: Map[T, Seq[(T, Double)]], vectorSpace: VectorSpace[T]): SSResult[T] = {
    val s = result.map {
      case (centroid, values) =>
        //if the cluster is of one values the Silhouette is 0
        val clusterSilhouette = values.flatMap {
          case (x, dist) =>
            val divisor = x match {
              case x: GroupedGroupFeature => x.N
              case _ => 1
            }
            val a_i = dist / divisor
            val b_ix = result.filter(_._1 != centroid).keys.map(c => vectorSpace.distance(c, x) / divisor)
            val b_i = if(b_ix.nonEmpty) b_ix.min else Double.NaN
            val s_i = if(a_i == 0 && b_i == 0) 0 else (b_i - a_i) / math.max(b_i, a_i)
            Seq.fill(divisor)(s_i)
        }
        centroid -> (clusterSilhouette.sum / clusterSilhouette.size, clusterSilhouette)
    }
    val overallSilhouette = s.flatMap(_._2._2)
    (s.map(c => c._1 -> c._2._1), overallSilhouette.sum / overallSilhouette.size)
  }

  /**
   *
   * @param result the clustering result
   * @return computes adjusted mutual information score with smile
   */
  private def ami[T](result: Map[T, Seq[(T, Double)]])(clusteringExtractor: ClusteringExtractor[T]): Double = {
    import smile.validation.metric.AdjustedMutualInformation
    import smile.validation.metric.AdjustedMutualInformation.Method
    val ami = new AdjustedMutualInformation(Method.MAX)
    val (y1, y2) = getContingency(result)(clusteringExtractor)
    ami.score(y1, y2)
  }

  /**
   *
   * @param result the clustering result
   * @return computes adjusted rand index score with smile
   */
  private def ari[T](result: Map[T, Seq[(T, Double)]])(clusteringExtractor: ClusteringExtractor[T]): Double = {
    import smile.validation.metric.AdjustedRandIndex
    val ari = new AdjustedRandIndex()
    val (y1, y2) = getContingency(result)(clusteringExtractor)
    ari.score(y1, y2)
  }

  /**
   * @param result a clustering result
   * @param clusteringExtractor clustering result extractor
   * @tparam T type of the clustered data
   * @return the arrays with predicted and true labels
   */
  private def getContingency[T](result: Map[T, Seq[(T, Double)]])(clusteringExtractor: ClusteringExtractor[T]): (Array[Int], Array[Int]) = {
    val seedIndices = result.values.flatMap(_.map(x => clusteringExtractor.className(x._1))).zipWithIndex.toMap
    val y1 = result.values.flatMap(r => r.map(r => r._1 -> seedIndices(clusteringExtractor.className(r._1)))).toSeq.sortBy(x => clusteringExtractor.ordering(x._1)).map(_._2).toArray

    val y2 = result.toSeq.zipWithIndex.flatMap {
      case ((_, values), i) => values.map(v => v._1 -> i)
    }.sortBy(x => clusteringExtractor.ordering(x._1)).map(_._2).toArray
    (y1, y2)
  }
}
