package it.unibo.big.streamprofiling.algorithm.app

/**
 * Class that runs an algorithm using a file of simulation specified in the conf.
 * on the window configuration given in the application.conf
 *
 */
object AlgorithmApp extends App {

    import it.unibo.big.streamprofiling.algorithm.execution.ClusteringAlgorithms
    import it.unibo.big.streamprofiling.algorithm.utils.Settings
    import Settings.CONFIG

    private val frequency = if (args.nonEmpty) args(0).toLong else CONFIG.getLong("window.frequency")
    private val numberOfSeeds = if (args.length > 1) args(1).toLong else CONFIG.getLong("window.seeds")
    private val simulationFileName = if (args.length > 2) args(2) else CONFIG.getString("window.simulation_file")
    private val windowDuration: Long = if (args.length > 3) args(3).toLong else CONFIG.getLong("window.windowDuration")
    private val slideDuration: Long = if (args.length > 4) args(4).toLong else CONFIG.getLong("window.slideDuration")
    private val DEBUG: Boolean = if (args.length > 6) args(6).toUpperCase() != "FALSE" else true

    private val windowStart: Option[Long] = if (args.length > 7) {
        val v = args(7).toLong
        if (v <= 0) None else Some(v)
    } else None
    private val windowEnd: Option[Long] = if (args.length > 8) {
        val v = args(8).toLong
        if (v <= 0) None else Some(v)
    } else None

    private val RANDOM_INIT: Boolean = if (args.length > 9) args(9).toUpperCase() != "FALSE" else true

    private val READ_AS_STREAM: Boolean = false

    //modify conf parameters
    Settings.set(Map(
        "window.frequency" -> frequency,
        "window.seeds" -> numberOfSeeds,
        "window.simulation_file" -> simulationFileName,
        "window.windowDuration" -> windowDuration,
        "window.slideDuration" -> slideDuration,
        "window.debugFolder" -> s"""${simulationFileName.replace(".csv", "")}""" //_window=${windowDuration}_slide=${slideDuration}"""
    ).map { case (x, y) => x -> y.asInstanceOf[AnyRef] })
    ClusteringAlgorithms.fromString(if (args.length > 5) args(5) else "")
        .compute(windowDuration, slideDuration, frequency, numberOfSeeds, inputSimulationFileName = Some(simulationFileName),
            readAsAStream = READ_AS_STREAM, debugResult = DEBUG, startTime = windowStart, endTime = windowEnd,
            randomInit = RANDOM_INIT)
}
