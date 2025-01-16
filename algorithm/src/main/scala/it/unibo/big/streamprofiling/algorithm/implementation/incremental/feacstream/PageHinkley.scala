package it.unibo.big.streamprofiling.algorithm.implementation.incremental.feacstream

/**
 * Object for Page-Hinkley test
 */
private [feacstream] object PageHinkley {
  import it.unibo.big.utils.Timer
  import it.unibo.big.utils.input.SchemaModeling.{ClusteringResult, TimestampedSchema}
  import org.slf4j.{Logger, LoggerFactory}

  /**
   * Trait that evidence the state of the statistical test
   */
  sealed trait PageHinkleyState

  private case object WARNING_STATE extends PageHinkleyState

  case object ALARM_STATE extends PageHinkleyState

  case object STATIONARY_STATE extends PageHinkleyState

  /**
   * Page hinkley test
   */
  class PageHinkleyTest {
    //Map of minimum MT value divided by cluster
    private var MT : Double = 0D
    //Previous MT
    private var mT : Double = 0D
    //Sum of PIBAR
    private var piBar_sum : Double = 0D
    //thresholds to update when a data partition is available
    private var delta : Double = 0 //toleranceThreshold
    private var lamdaAlarm: Double = 0
    private var lamdaWarning: Double = 0
    //index of a value
    private var clusterGeneration : Int = 0
    //debug variables
    private val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)
    private var previousState: PageHinkleyState = STATIONARY_STATE

    /**
     * Define the alarm parameters when a data partition is available and also
     * update the distances from centroids, given from the clustering result.
     *
     * @param clusters         a clustering result
     * @param clusteringResult a clustering result
     * @param time             time of computation of the clustering result
     */
    def initParameters(clusters: FeacStreamClusteringResult, clusteringResult: ClusteringResult, time: Long): Unit = {
      val radii = clusters.clusters.map(_._2.clusterRadiusFeac)
      lamdaAlarm = radii.sum / radii.size
      lamdaWarning = lamdaAlarm / 3
      delta = 0.1 * lamdaAlarm
      mT = 0D
      MT = 0D
      piBar_sum = 0D

      val schemas = clusteringResult.flatMap {
        case (_, xs) => val values = xs.collect{
          case (s: TimestampedSchema, d) => (s.timestamp.getTime, d)
        }
        values
      }.toSeq.sortBy(_._1)

      schemas.foreach{ case (_, d) => updateData(d) }
    }

    /**
     *
     * @param distance the distance to add to the monitoring
     * @return the state for the actual page hinkley (warning, alarm or normal)
     */
    def compute(distance: Double): PageHinkleyState = {
      updateData(distance)
      //LOGGER.debug(s"PH test parameters mT = $mT, minMT = $minMT, lamda Alarm = $lamdaAlarm, lamda warning = $lamdaWarning")
      val newState = if (mT - MT > lamdaAlarm) {
        ALARM_STATE
      } else if (mT <= MT + lamdaWarning) {
        STATIONARY_STATE
      } else {
        WARNING_STATE
      }
      if(previousState != newState) {
        LOGGER.debug(s"Transition from $previousState to $newState")
      }
      previousState = newState
      newState
    }

    /**
     * @param pi average Euclidean distance between object and closest center at time i
     */
    private def updateData(pi: Double): Unit = {
      clusterGeneration += 1
      LOGGER.info("Start update data in PH test")
      Timer.start()
      //calculate p and mT
      piBar_sum += pi
      mT += pi - (piBar_sum / clusterGeneration) - delta

      //check min mt values
      MT = math.min(MT, mT)
      LOGGER.info(s"End update data in PH test ${Timer.getElapsedTime}")
    }
  }
}
