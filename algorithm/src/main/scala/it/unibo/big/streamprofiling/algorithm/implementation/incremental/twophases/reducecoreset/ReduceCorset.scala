package it.unibo.big.streamprofiling.algorithm.implementation.incremental.twophases.reducecoreset

/**
 * Object for different implementation of reduce coreset
 */
object ReduceCorset {

    import it.unibo.big.streamprofiling.algorithm.implementation.incremental.groupfeature.{BucketWindowGroupFeature, GroupedGroupFeature}
    import it.unibo.big.streamprofiling.algorithm.implementation.incremental.twophases.Debug2PhasesAlgorithmsUtils.{OperationDebugInfo, REDUCE_CORESET}
    import it.unibo.big.streamprofiling.algorithm.implementation.incremental.twophases.TwoPhasesDataTypes.{TwoPhasesClusterAssignments, TwoPhasesClusteringResult, TwoPhasesHashTable}
    import org.slf4j.{Logger, LoggerFactory}

    import scala.collection.mutable

  private val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  /**
   * Removing the bucket from the result (used just for merge two buckets)
   *
   * @param bucket  the bucket to analyze
   * @param cluster the cluster of the bucket
   */
  private def removeFromCluster[T](bucket: IndexedSeq[T], cluster: GroupedGroupFeature, clusteringResult: TwoPhasesClusteringResult): TwoPhasesClusteringResult = {
    clusteringResult + (cluster -> clusteringResult(cluster).filter(_._1.asInstanceOf[BucketWindowGroupFeature[T]].bucket != bucket))
  }

  /**
   *
   * @param clusteringResult input clustering result
   * @param hashTable input hash table
   * @param clusterAssignments of new data and cluster centroids to update
   * @param applyReduceBucketHeuristic true if you want to apply the heuristic to reduce bucket numbers, avoiding comparing the distances of all buckets
   * @param m coreset size
   * @param similarityBucketFunction the similarity function for merge similar buckets
   * @return updated clustering result, update hash table and updated cluster assignments and the information of the operation
   */
  def reduceCoreset[T](clusteringResult: TwoPhasesClusteringResult, hashTable: TwoPhasesHashTable[T], clusterAssignments: TwoPhasesClusterAssignments[T],
                       applyReduceBucketHeuristic: Boolean, m: Int, similarityBucketFunction: (BucketWindowGroupFeature[T], BucketWindowGroupFeature[T]) => Double):
  (TwoPhasesClusteringResult, TwoPhasesHashTable[T], TwoPhasesClusterAssignments[T], OperationDebugInfo) = {

    LOGGER.info(s"Reduce buckets (size = ${hashTable.size}) to $m")
    val time = System.currentTimeMillis()
    var newClusteringResult = clusteringResult
    var newHashTable = hashTable
    var newClusterAssignments = clusterAssignments

    LOGGER.debug("start create distance map")
    val distancesMap = if (applyReduceBucketHeuristic) {
      val zippedMap = newHashTable.zipWithIndex.map{case (v, i) => i -> v}
      zippedMap.collect {
        case (i, (b1, v1)) if i < zippedMap.size - 1 =>
          val (b2, v2) = zippedMap(i + 1)
          b1 -> b2 -> similarityBucketFunction(v1._1, v2._1)
      }
    } else {
      newHashTable.flatMap {
        case (b1, v1) =>
          newHashTable.filterKeys(_ != b1).map {
            case (b2, v2) => b1 -> b2 -> similarityBucketFunction(v1._1, v2._1)
          }
      }
    }
    LOGGER.debug("end create distance map")

    LOGGER.debug("start create priority queue map")
    val distances : mutable.PriorityQueue[((IndexedSeq[T], IndexedSeq[T]), Double)] = new mutable.PriorityQueue[((IndexedSeq[T], IndexedSeq[T]), Double)]()(new Ordering[((IndexedSeq[T], IndexedSeq[T]), Double)] {
      override def compare(x: ((IndexedSeq[T], IndexedSeq[T]), Double), y:((IndexedSeq[T], IndexedSeq[T]), Double)): Int = x._2.compare(y._2)
    }.reverse)
    distances ++= distancesMap.toSeq
    LOGGER.debug("end create priority queue map")

    var mergedBuckets : Set[IndexedSeq[T]] = Set() //set of merged buckets
    /**
     * Update distances Map considering the parameters and the fact the applyReduceBucketHeuristic is applied
     * @param merged the merged hash value
     * @param fixed the remained hash value
     */
    def removeMergedElementDistance(merged: IndexedSeq[T], fixed: IndexedSeq[T]): Unit = {
      if(applyReduceBucketHeuristic) {
        distances ++= distances.filter(x => x._1._1 == merged || x._1._2 == merged).collect {
          case ((`merged`, a), _) if a != fixed && !mergedBuckets.contains(a) => ((fixed, a), similarityBucketFunction(newHashTable(a)._1, newHashTable(fixed)._1))
          case ((a, `merged`), _) if a != fixed && !mergedBuckets.contains(a) => ((a, fixed), similarityBucketFunction(newHashTable(a)._1, newHashTable(fixed)._1))
        }
      }
      mergedBuckets += merged
    }

    while (newHashTable.size > m) {
      var x: IndexedSeq[T] = IndexedSeq()
      var y: IndexedSeq[T] = IndexedSeq()
      var d: Double = 0D
      do { //find buckets to merge, not considering already merged buckets
        val r = distances.dequeue()
        x = r._1._1
        y = r._1._2
        d = r._2
      } while(mergedBuckets.contains(x) || mergedBuckets.contains(y))
      val r = mergeBuckets(x, Set(x, y), newClusteringResult, newHashTable, newClusterAssignments)
      //update result values
      newClusteringResult = r._1
      newHashTable = r._2
      newClusterAssignments = r._3
      //update distances
      val merged = if (r._4 == x) y else x
      val fixed = if (r._4 == x) x else y
      removeMergedElementDistance(merged, fixed)

      LOGGER.debug(s"Merge buckets $fixed and $merged in cluster (distance = $d)")
    }
    val totalTime = System.currentTimeMillis() - time
    LOGGER.debug(s"Reduce coreset took $totalTime ms for ${hashTable.size - m} buckets")
    (newClusteringResult, newHashTable, newClusterAssignments, OperationDebugInfo(REDUCE_CORESET, hashTable.size - m, totalTime))
  }

  /**
   *
   * @param clusteringResult           input clustering result
   * @param hashTable                  input hash table
   * @param clusterAssignments         of new data and cluster centroids to update
   * @param m                          coreset size
   * @param l                          the number of hash function (length of buckets)
   * @return updated clustering result, update hash table and updated cluster assignments and the operation debug information
   */
  def reduceCoresetWithHash[T](clusteringResult: TwoPhasesClusteringResult, hashTable: TwoPhasesHashTable[T],
                               clusterAssignments: TwoPhasesClusterAssignments[T], m: Int, l: Int): (TwoPhasesClusteringResult, TwoPhasesHashTable[T], TwoPhasesClusterAssignments[T], OperationDebugInfo) = {

    val time = System.currentTimeMillis()
    var newClusteringResult = clusteringResult
    var newHashTable = hashTable
    var newClusterAssignments = clusterAssignments
    //set number of hash has l
    var numberOfHashes = l

    while (newHashTable.size > m) {
      LOGGER.info(s"Reduce buckets (size = ${newHashTable.size}) to $m use numberOfHashes = $numberOfHashes (l = $l)")
      numberOfHashes -= 1 //reduce number of hashes for merge same buckets
      //group data with first numberOfHashValues are the same
      newHashTable.groupBy(_._1.slice(0, numberOfHashes)).foreach{
        case (group, xs) =>
          val buckets = xs.keys
          val r = mergeBuckets(buckets.head, buckets.toSet, newClusteringResult, newHashTable, newClusterAssignments)
          //update result values
          newClusteringResult = r._1
          newHashTable = r._2
          newClusterAssignments = r._3
          LOGGER.debug(s"Merge ${buckets.size} buckets of group $group in cluster (numberOfHashes = $numberOfHashes)")
      }
    }
    (newClusteringResult, newHashTable, newClusterAssignments, OperationDebugInfo(REDUCE_CORESET, hashTable.size - m, System.currentTimeMillis() - time))
  }

  /**
   *
   * @param proposedX the candidate element to be the merged bucket
   * @param valuesToMerge the set of hash values to merge, contains also the proposed X
   * @param clusteringResult  input clustering result
   * @param hashTable input hash table
   * @param clusterAssignments  of new data and cluster centroids to update
   * @tparam T type of buckets
   * @return updated clustering result, update hash table and updated cluster assignments and the used bucket for merge the values
   */
  private def mergeBuckets[T](proposedX: IndexedSeq[T], valuesToMerge: Set[IndexedSeq[T]], clusteringResult: TwoPhasesClusteringResult, hashTable: TwoPhasesHashTable[T],
                              clusterAssignments: TwoPhasesClusterAssignments[T]): (TwoPhasesClusteringResult, TwoPhasesHashTable[T], TwoPhasesClusterAssignments[T], IndexedSeq[T]) = {
    var newHashTable = hashTable
    var newClusteringResult = clusteringResult
    var newClusterAssignments = clusterAssignments

    //merge buckets
    var x = proposedX
    var (xFeature, xCentroid) = newHashTable(x)

    val otherClusterData = newHashTable.filterKeys(valuesToMerge.contains).filter(_._2._2.nonEmpty)

    //merge preserves old clusters
    if (xCentroid.isEmpty && otherClusterData.nonEmpty) {
      val r = otherClusterData.head
      x = r._1
      xFeature = r._2._1
      xCentroid = r._2._2
    }
    var mergedFeature: BucketWindowGroupFeature[T] = xFeature
    valuesToMerge.foreach(h => if (h != x) {
      mergedFeature = mergedFeature.add[BucketWindowGroupFeature[T]](newHashTable(h)._1)
    })

    //remove data from clustering result if present
    if (xCentroid.isDefined) {
      valuesToMerge.foreach(h => {
        val (_, c) = newHashTable(h)
        if (c.isDefined) {
          newClusteringResult = removeFromCluster(h, c.get, newClusteringResult)
        }
      })
      //merge buckets and insert into the clustering result
      newClusteringResult += xCentroid.get -> (newClusteringResult(xCentroid.get) :+ (mergedFeature -> 0D))
    }

    newHashTable += x -> (mergedFeature, xCentroid)

    valuesToMerge.foreach(h => if (h != x) {
      newHashTable -= h
    })

    // update newClusterAssignments
    if (otherClusterData.nonEmpty) { //there are no new group features
      newClusterAssignments += xCentroid.get ->
        (newClusterAssignments.getOrElse(xCentroid.get, Seq()) ++
          otherClusterData.flatMap(oc => newClusterAssignments.getOrElse(oc._2._2.get, Seq())))
    } else if (xCentroid.isDefined) {
      newClusterAssignments += xCentroid.get ->
        newClusterAssignments.getOrElse(xCentroid.get, Seq())
    }

    (newClusteringResult, newHashTable, newClusterAssignments, x)
  }
}
