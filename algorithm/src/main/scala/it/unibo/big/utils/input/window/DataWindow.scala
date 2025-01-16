package it.unibo.big.utils.input.window

/**
 * Utility object for compute a window in scala, without Spark support
 */
object DataWindow {
  import it.unibo.big.utils.input.SchemaModeling.{SchemaWithTimestamp, Window}
  import org.slf4j.{Logger, LoggerFactory}

  import java.sql.Timestamp

  private val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  /**
   * Method to compute a window on timed data
   * @param data the timestamped dataset, sorted by timestamp and value
   * @param windowDuration the duration of the window
   * @param slideDuration the duration of the slide
   * @param windowAction the action to compute for a given window and window data
   * @param windowStart    if present is the simulation start time, otherwise start from the first instance in seq
   * @param windowEnd    if present is the simulation end time, otherwise end with the last instance in seq
   * @param numberOfWindowsToConsider if present is the number of windows to consider
   */
  def windowing[T <: SchemaWithTimestamp](data: Iterator[T], windowDuration: Long, slideDuration: Long,
                windowAction: (Window, WindowData[T], Long) => Unit, windowStart: Option[Long], windowEnd: Option[Long], numberOfWindowsToConsider: Option[Int] = None): Unit = {
    require((windowDuration % slideDuration == 0) && slideDuration <= windowDuration)
    val dataset = data//.sortBy(x => (x.timestamp.getTime, x.value))

    var window: Option[Window] = None
    var windowTime = 0L
    var windowDataPanes = Map[Long, Seq[T]]() //map that for each slide start time has data
    var windowIt = 0
    var finishWithWindowEnd = false

    if(numberOfWindowsToConsider.isDefined) {
      require(numberOfWindowsToConsider.get > 0 && windowEnd.isEmpty, "Number of windows to consider must be greater than 0")
    }
    if(windowEnd.isDefined) {
      require(windowEnd.get > 0 && numberOfWindowsToConsider.isEmpty, "Window end must be greater than 0")
    }
    var numberOfWindows = 0

    /**
     *
     * @param d schema to add to window data pane structure
     */
    def addSchemaInDataPaneStructure(d: T): Unit = {
      val paneD = window.get.paneStart(d).get
      if (!windowDataPanes.contains(paneD)) {
        windowDataPanes += paneD -> Seq[T]()
      }
      windowDataPanes += paneD -> (windowDataPanes(paneD) :+ d)
    }
    LOGGER.info("Start iterating on data")
    while(dataset.hasNext && !finishWithWindowEnd) {
      if(!dataset.hasNext) {
        LOGGER.info("Waiting for data")
      } else {
        val d = dataset.next()
        //define window end threshold if present
        if (windowEnd.nonEmpty) {
          if (d.timestamp.getTime > windowEnd.get) {
            LOGGER.info("Window end reached")
            finishWithWindowEnd = true
          }
        }
        if (!finishWithWindowEnd) {
          if (window.isEmpty) {
            windowTime = d.timestamp.getTime
          }
          if (window.isEmpty && windowStart.isEmpty) {
            //create a window starting from the last pane, the first panes are empty -- only if window start is not defined
            window = Some(Window(
              start = new Timestamp(d.timestamp.getTime - (((windowDuration / slideDuration) - 1) * slideDuration)),
              end = new Timestamp(d.timestamp.getTime + slideDuration), slideDuration
            ))
          }

          var insertData = true
          if (windowStart.isDefined) { //define inserting of data if window start is defined
            insertData = d.timestamp.getTime >= windowStart.get
          }
          if (insertData) {
            //if the window start is defined and the window is empty create the window
            if (windowStart.isDefined && window.isEmpty) {
              window = Some(Window(
                start = new Timestamp(windowStart.get),
                end = new Timestamp(windowStart.get + windowDuration),
                slideDuration,
                isFirstCompleteWindow = true
              ))
            }
            if (window.get.contains(d.timestamp)) {
              addSchemaInDataPaneStructure(d)
            } else {
              var inserted = false
              do {
                // launch a computation on windowData
                //val isFirstCompleteWindow = windowData.contains(dataset.head) && window.end.getTime - dataset.head.timestamp.getTime == windowDuration
                val oldWindowStart = window.get.start.getTime
                numberOfWindows += 1
                windowAction(window.get, getDataFromWindow(window.get, slideDuration, windowDataPanes), windowTime + (slideDuration * windowIt))
                if (numberOfWindowsToConsider.nonEmpty) {
                  if (numberOfWindows >= numberOfWindowsToConsider.get) {
                    LOGGER.info("Number of windows reached")
                    finishWithWindowEnd = true
                  }
                }
                windowIt += 1
                window = Some(window.get.slide)
                //keep only the last pane before the window
                windowDataPanes = windowDataPanes.filterKeys(_ >= oldWindowStart)
                if (window.get.contains(d.timestamp)) {
                  inserted = true
                  addSchemaInDataPaneStructure(d)
                }
                if(window.get.start.getTime > d.timestamp.getTime) {
                  //if the window start is greater than the timestamp, the element is skipped
                  inserted = true
                }
              } while (!inserted && !finishWithWindowEnd) //do-while for ensure that new data is in the new window
            }
          }
        } else if (window.nonEmpty) {
          numberOfWindows += 1
          windowAction(window.get, getDataFromWindow(window.get, slideDuration, windowDataPanes), windowTime + (slideDuration * windowIt))
        }
      }
    }
  }

  /**
   * Data structure for the window data
   * @param windowRecords all the records in the window
   * @param newPaneRecords new pane records
   * @param oldPaneRecords old pane records
   * @param windowRecordsWithoutLastPane all the records in the window without the new pane
   * @tparam T type of schema
   */
  case class WindowData[T <: SchemaWithTimestamp](windowRecords: Seq[T], newPaneRecords: Seq[T], oldPaneRecords: Seq[T], windowRecordsWithoutLastPane: Seq[T])

  /**
   *
   * @param window          a window
   * @param slideDuration   window slide duration
   * @param windowDataPanes panes data of the window
   * @return 3 sets for the window, one regarding all the window, one the newest data and one the last pane
   */
  private def getDataFromWindow[T <: SchemaWithTimestamp](window: Window, slideDuration: Long, windowDataPanes: Map[Long, Seq[T]]): WindowData[T] = {
    val windowRecords = windowDataPanes.filterKeys(window.contains).values.flatten.toSeq
    val newPaneRecords = if (window.isFirstCompleteWindow) windowRecords else windowDataPanes.getOrElse(window.end.getTime - slideDuration, Seq())
    val oldPaneRecords = windowDataPanes.getOrElse(window.start.getTime - slideDuration, Seq())
    val windowRecordsWithoutLastPane = windowDataPanes.filterKeys(x => window.contains(x) && x != window.start.getTime - slideDuration).values.flatten.toSeq
    LOGGER.info(s"Start action with window  $window with total data = ${windowRecords.size} (new data = ${newPaneRecords.size}, old data = ${oldPaneRecords.size})")
    WindowData(windowRecords, newPaneRecords, oldPaneRecords, windowRecordsWithoutLastPane)
  }
}
