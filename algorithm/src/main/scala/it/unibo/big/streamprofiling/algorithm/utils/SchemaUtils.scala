package it.unibo.big.streamprofiling.algorithm.utils

import it.unibo.big.utils.input.SchemaModeling.Schema

/**
 * A utility object for work with schemas
 */
object SchemaUtils {
  /**
   * The definition of a distance map
   */
  type DistanceMap = Map[(Schema, Schema), Double]

  /**
   *
   * @param records the input records
   * @param previousMaps if present to reuse
   * @param distanceFunction the used distance function for compute the map
   * @return the distance map of each records
   */
  def distanceMap(records : Seq[Schema], previousMaps: Map[Set[Schema], DistanceMap] = Map(), distanceFunction : (Schema, Schema) => Double = (a, b) => a.jaccard(b)): DistanceMap = {
    var map = Map[(Schema, Schema), Double]()
    for((r1, i) <- records.zipWithIndex;
        (r2, j) <- records.zipWithIndex) {
      if(i <= j) {
        val previousMap = previousMaps.filterKeys(x => x.contains(r1) && x.contains(r2))
        val distance = if(previousMap.nonEmpty) {
          //retrieve distance from a previous distance map
          //previousMap.head._2.filterKeys(x => x.productIterator.contains(Seq(r1, r2))).head._2
          previousMap.head._2((r1, r2))
        } else if(i == j) 0 else distanceFunction(r1, r2)
        map += r1 -> r2 -> distance
        map += r2 -> r1 -> distance
      }
    }
    map
  }

  /**
   *
   * @param map the input distance map
   * @param values values of want to calculate the distance, if empty (default) use all matrix values
   * @return the schema that is less distant from all (or only values if specified) - i.e. the media
   */
  def bestOf(map: DistanceMap, values: Seq[Schema] = Seq()): Schema = {
    val elements = (if(values.isEmpty) map.keySet.map(_._1) else values).toSeq
    elements.map(e1 =>  e1 -> elements.map(e2 => map((e1, e2))).sum / (elements.size - 1)).minBy(_._2)._1
  }
}
