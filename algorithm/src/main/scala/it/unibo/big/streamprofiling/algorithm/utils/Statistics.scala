package it.unibo.big.streamprofiling.algorithm.utils

/**
 * Properties to monitor in the algorithms
 */
object MonitoringProperty extends Enumeration {
  type MonitoringProperty = Value
  val NUMBER_OF_CHANGE_DICTIONARY,
  NUMBER_OF_REDUCE_CORESET,
  NUMBER_OF_INPUT_BUCKETS,
  NUMBER_OF_RECLUSTERING,
  NUMBER_OF_BUCKETS,
  TIME_FOR_CLUSTERING,
  TIME_FOR_CHANGE_DICTIONARY,
  TIME_FOR_REMOVE_OLD_RECORDS,
  TIME_FOR_FIND_CLUSTER_TO_SPLIT_MERGE,
  TIME_FOR_MERGE,
  TIME_FOR_SPLIT,
  TIME_FOR_UPDATE_HASH_TABLE_START,
  TIME_FOR_WHOLE_RECLUSTERING,
  TIME_FOR_GENERATE_NEW_WINDOW,
  TIME_FOR_REDUCE_CORESET,
  TIME_FOR_UPDATE_RESULT,
  TIME_FOR_UPDATE_DATA_STRUCTURE,
  TIME_FOR_TEST_RESULT,
  NUMBER_OF_FIND_CLUSTER_TO_SPLIT_MERGE,
  TIME_FOR_CALCULATE_FOR_SPLIT_OPERATION,
  TIME_FOR_CALCULATE_FOR_MERGE_OPERATION,
  TIME_FOR_UPDATE_CLUSTERS,
  TIME_FOR_CALCULATE_DISTANCES_BUCKET_CLUSTERS,
  TIME_FOR_ASSIGN_HASH = Value
}

/**
 * Class for monitoring the statistics of the algorithms
 * @param properties the properties to monitor in the algorithms as a map
 */
case class MonitoringStats(properties: Map[MonitoringProperty.Value, Any] = Map.empty) {
  def getAllValues: Map[MonitoringProperty.Value, Any] = {
    MonitoringProperty.values.toSeq.map(property => property -> getValue(property)).toMap
  }

  def getValue(property: MonitoringProperty.Value): Any = properties.getOrElse(property, 0)

  def updateValue(property: MonitoringProperty.Value, value: Any, replace: Boolean = false): MonitoringStats = {
    val updatedProperties =
      if (replace || !properties.contains(property)) properties + (property -> value)
      else properties.updated(property, combineValues(properties(property), value))

    MonitoringStats(updatedProperties)
  }

  private def combineValues(existingValue: Any, newValue: Any): Any = (existingValue, newValue) match {
    case (i: Int, j: Int) => i + j
    case (l: Long, m: Long) => l + m
    case _ => newValue
  }
}

/**
 * Companion object for the MonitoringStats class
 */
object MonitoringStats {
  def apply(): MonitoringStats = new MonitoringStats()
}