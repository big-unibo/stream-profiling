package it.unibo.big.streamprofiling.algorithm.implementation.incremental.groupfeature

import it.unibo.big.streamprofiling.algorithm.utils.MapUtils._
import it.unibo.big.utils.input.SchemaModeling.{Schema, Window}

/**
 * Abstract class for group feature, with empty initial schemas
 * @param timestamp the last timestamp
 */
private [algorithm] abstract class GroupedGroupFeature(timestamp: Long) extends GroupFeature(timestamp, Seq())

/**
 * Class for group feature
 * @param timestamp the last timestamp
 * @param initialSize initial size of the group feature
 * @param startingLS initial linear sum of the group feature
 * @param startingSS initial sum of square of the group feature
 */
private [incremental] class RawGroupedGroupFeature(timestamp: Long,
                                                 override val initialSize: Int,
                                                 override val startingLS: Map[String, Double],
                                                 override val startingSS: Map[String, Double]) extends GroupedGroupFeature(timestamp)

/**
 * Class that represent a group of group features
 * @param groupFeatures the group features
 */
private [incremental] class GroupedGroupFeatureAggregator(groupFeatures: Map[Long, GroupFeature]) extends GroupedGroupFeature(if(groupFeatures.nonEmpty) groupFeatures.keys.max else 0L) {
  override lazy val initialSize: Int = groupFeatures.map(_._2.N).sum

  override lazy val startingLS: Map[String, Double] = if (groupFeatures.isEmpty) Map() else {
    groupFeatures.map(_._2.LS).reduce(sumDouble)
  }

  override lazy val startingSS: Map[String, Double] = if (groupFeatures.isEmpty) Map() else {
    groupFeatures.map(_._2.SS).reduce(sumDouble)
  }
}

/**
 * Companion object of GroupedGroupFeatureByPanes, with auxiliary constructors
 */
private [incremental] object GroupedGroupFeatureAggregator {
  /**
   *
   * @param ps input group features
   * @return the merged group feature that is the centroid
   */
  def center(ps: Seq[GroupedGroupFeature]): GroupedGroupFeatureAggregator = {
    var centroid: GroupedGroupFeatureAggregator = GroupedGroupFeatureAggregator(Map())
    for (p <- ps) { centroid = centroid.add(p) }
    centroid
  }

  /**
   *
   * @param panes panes to consider
   * @return a new object with the panes
   */
  def apply(panes: Map[Long, GroupFeature]): GroupedGroupFeatureAggregator = new GroupedGroupFeatureAggregator(panes)
}

/**
 * A case class that represent both:
 * - GFs of an hash value (bucket) in a cluster
 * - Cluster centroid with a none bucket
 * @param bucket the bucket, if none it is a centroid
 * @param panesTmp the initial panel group features
 * @tparam T type of element inside the bucket
 */
private [incremental] case class BucketWindowGroupFeature[T](bucket: IndexedSeq[T], private var panesTmp: Map[Long, GroupFeature]) extends GroupedGroupFeatureAggregator(panesTmp) {

  override def schemas: Seq[Schema] = panes.flatMap(_._2.schemas).toSeq

  /**
   *
   * @return panes group features inside the BucketWindowFeature, if it has a bucket it must have a value for each pane.
   *         If it is a centroid not matter.
   */
  def panes: Map[Long, GroupFeature] = panesTmp

  /**
   *
   * @return true if all the panes are empty
   */
  def isEmpty: Boolean = panes.isEmpty

  /**
   * @param window the group feature window
   * @return true if all the group features contained are in the window, false if there are expired data
   */
  def isAllContainedBy(window: Window): Boolean = panes.keySet.forall(window.contains)

  /**
   *
   * @param window window to consider
   * @return a new object with updated data and centroid
   */
  def generateNewFrom(window: Window): BucketWindowGroupFeature[T] = BucketWindowGroupFeature(bucket, panes.filter(x => window.contains(x._1)))

  /**
   *
   * @param window window to consider
   * @return true if exist a pane in the window
   */
  def existIn(window: Window): Boolean = panes.keySet.exists(window.contains)

  /**
   *
   * @param cf the group feature to add
   *           Update the group feature adding cf
   * @tparam GF the other gf type
   * @return the merged gfs, considering that in case of two buckets the add procedure is different: have to merge panes
   */
  override def add[GF <: GroupFeature](cf: GroupFeature): GF = {
    val xPanes = panes
    cf match {
      case y: BucketWindowGroupFeature[T] =>
        val yPanes = y.panes
        panesTmp = xPanes.map {
          case (t, b) if yPanes.contains(t) => t -> b.add(yPanes(t))
          case (t, b) => t -> b
        } ++ yPanes.filterKeys(!xPanes.contains(_))
      case y: SimplifiedGroupFeature =>
        //for centroid preserve panes without add
        if(xPanes.contains(y.timestamp)) {
          //val selectedPane = xPanes(t)
          panesTmp += y.timestamp -> panesTmp(y.timestamp).add(y)
        } else {
          panesTmp += y.timestamp -> y
        }
    }
    super.addWithoutSchemas(cf) //use simplified method and avoid to compute add of schemas
  }

  override def add(s: Schema, t: Long): Unit = throw new UnsupportedOperationException()
}

/**
 * Util object for group feature
 */
private [incremental]  object GroupFeatureUtils {

  /**
   * Ordering for group feature
   * @tparam T type of the group feature
   * @return the ordering considering the toString of the group feature
   */
  implicit def bucketOrdering[T]: Ordering[IndexedSeq[T]] = new Ordering[IndexedSeq[T]] {
      override def compare(x: IndexedSeq[T], y: IndexedSeq[T]): Int = x.toString compare y.toString
    }
}

/**
 * Class for group feature in a bucket.
 * @param timestamp the last timestamp
 * @param schemasTmp   initial schemas in the group feature
 */
private [incremental]  case class SimplifiedGroupFeature(timestamp: Long, private var schemasTmp: Seq[Schema]) extends GroupFeature(timestamp, schemasTmp) {
  override lazy val initialSize: Int = schemasTmp.size
  override lazy val startingLS : Map[String, Double] = schemasTmp.map(_.values).reduce(sumDouble)
  override lazy val startingSS : Map[String, Double] = square(startingLS)
}
