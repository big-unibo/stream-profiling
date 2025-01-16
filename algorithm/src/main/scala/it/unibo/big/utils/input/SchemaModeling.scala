package it.unibo.big.utils.input

object SchemaModeling {
  import java.sql.Timestamp

  type ClusteringResult = Map[Schema, Seq[(Schema, Double)]]

  /**
   * The schema trait
   */
  trait Schema {
    /**
     * Name of the seed that have generated the schema
     */
    val seedName: String
    /**
     * Schema values
     */
    val values: Map[String, Double]

    /**
     *
     * @return the values of the schema in a particular formatted string "[a:prob, b:prob, c:prob]"
     */
    lazy val value: String = s"[${values.map{
      case (k, v) => s"$k:${"%.2f".format(v)}"
    }.mkString(",")}]"

    /**
     * Definition of the jaccard distance between schemas
     *
     * @param other the other schema
     * @return the jaccard distance with this and other
     */
    def jaccard(other: Schema): Double = 1 - jaccardSimilarity(other)

    /**
     * Definition of the jaccard similarity between schemas
     *
     * @param other the other schema
     * @return the weighted jaccard similarity with this and other
     */
    private def jaccardSimilarity(other: Schema): Double = {
      val dictionary: Seq[String] = (values.keys ++ other.values.keys).toSeq.distinct
      val num = dictionary.map(s => math.min(values.getOrElse(s, 0D), other.values.getOrElse(s, 0D))).sum
      val den = dictionary.map(s => math.max(values.getOrElse(s, 0D), other.values.getOrElse(s, 0D))).sum
      num / den
    }

    /**
     * @param other the other schema
     * @return Euclidean distance considering the dictionary
     */
    def euclideanDistance(other: Schema): Double = math.sqrt(squaredEuclideanDistance(other))

    /**
     * @param other the other schema
     * @return squared Euclidean distance considering the dictionary
     */
    private def squaredEuclideanDistance(other: Schema): Double = {
      (values.keySet ++ other.values.keySet).toSeq.map(i => math.pow(values.getOrElse(i, 0D) - other.values.getOrElse(i, 0D), 2)).sum
    }
  }

  /**
   * A basic schema class, without seed name
   *
   * @param values the values of the schema
   */
  case class BasicSchema(id: Int, override val values: Map[String, Double]) extends Schema {
    override val seedName: String = "no-name"
  }

  object BasicSchema{
    private var COUNT = 0

    def apply(values: Set[String]) : BasicSchema = {
      BasicSchema(values.map(s => s -> 1D).toMap)
    }

    def apply(values: Map[String, Double]): BasicSchema = {
      COUNT += 1
      BasicSchema(COUNT, values)
    }

    def apply(values: IndexedSeq[Double], dictionary: IndexedSeq[String]): BasicSchema = {
      BasicSchema(dictionary.indices.map(i => dictionary(i) -> values(i)).toMap)
    }
  }

  /**
   * Window definition
   *
   * @param start time
   * @param end   time
   * @param period the period of the window, to slide
   * @param isFirstCompleteWindow true if the window is the first complete window, false otherwise (default is false)
   */
  case class Window(start: Timestamp, end: Timestamp, period: Long, isFirstCompleteWindow: Boolean = false) {

    /**
     *
     * @param schema a timestamped schema
     * @return an option that tells the pane start of that schema, if the schema is in that window
     */
    def paneStart(schema: SchemaWithTimestamp): Option[Long] = {
      val t = schema.timestamp.getTime
      if(!contains(t)) {
        None
      } else {
        var paneStart = start.getTime
        var paneEnd = paneStart + period
        var find = false
        while(paneEnd <= end.getTime && !find) {
          if(t >= paneStart && t < paneEnd) {
            find = true
          }
          if(!find) {
            paneStart = paneEnd
            paneEnd += period
          }
        }
        if (find) Some(paneStart) else None
      }
    }

    /**
     * @param startSimulation the start of the simulation
     * @return the panes of the window that are after the start of the simulation
     */
    def panes(startSimulation: Long): Seq[Long] = start.getTime.until(end.getTime, period).filter(_ >= startSimulation)

    /**
     *
     * @return a slide window: add period duration to both start and end time
     */
    def slide: Window = Window(new Timestamp(start.getTime + period), new Timestamp(end.getTime + period), period)

    /**
     *
     * @param t a timestamp
     * @return true if the timestamp is in window interval
     */
    def contains(t: Timestamp): Boolean = contains(t.getTime)

    /**
     *
     * @param t a timestamp
     * @return true if the timestamp is in window interval
     */
    def contains(t: Long): Boolean = t >= start.getTime && t < end.getTime

    /**
     *
     * @return the time of the pane
     */
    def paneTime: Long = end.getTime - period

    /**
     *
     * @return the window length
     */
    def length: Long = end.getTime - start.getTime
  }

  abstract class SchemaWithTimestamp extends Schema {
    def timestamp: Timestamp
  }

  /**
   * Schema read in a window
   *
   * @param stringValues values of the schema (i.e. the payload of the received kafka message)
   * @param timestamp the timestamp the schema has been received
   * @param seedName  the seed of the received schema (i.e. the key of the received kafka message)
   */
  case class TimestampedSchema(private val stringValues: Set[String], timestamp: Timestamp, seedName: String) extends SchemaWithTimestamp {
    override val values: Map[String, Double] = stringValues.map(s => s -> 1D).toMap
  }
}
