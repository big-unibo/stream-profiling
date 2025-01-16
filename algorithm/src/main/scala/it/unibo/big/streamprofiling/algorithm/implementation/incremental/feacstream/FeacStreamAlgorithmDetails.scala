package it.unibo.big.streamprofiling.algorithm.implementation.incremental.feacstream

/**
 * Implementation details of FEAC algorithm
 * */
private [feacstream] object FeacStreamAlgorithmDetails {
  import MutationOperations._
  import PageHinkley._
  import it.unibo.big.streamprofiling.algorithm.Metrics.{SSResult, simplifiedSilhouette}
  import it.unibo.big.streamprofiling.algorithm.implementation.kmeans.KMeans.{ResultManipulation, kMeansPP}
  import it.unibo.big.streamprofiling.algorithm.implementation.kmeans.{KMeans, KMeansOnSchemas}
  import it.unibo.big.streamprofiling.algorithm.utils.RandomUtils.pickRandom
  import it.unibo.big.streamprofiling.algorithm.utils.VectorSpace.VectorSpace
  import it.unibo.big.streamprofiling.algorithm.utils.{MonitoringProperty, MonitoringStats}
  import it.unibo.big.utils.input.SchemaModeling.{ClusteringResult, Schema, SchemaWithTimestamp, Window}
  import org.slf4j.{Logger, LoggerFactory}

  private val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)
  private val SMALL_FITNESS_IMPROVEMENT: Double = 0.001
  private val MAX_NUMBER_OF_GENERATIONS : Int = 50
  private val NUMBER_OF_SMALL_FITNESS_IMPROVEMENT : Int = 10
  private var isInAlarm = false //when true not compute the ph test and wait for reclustering

  /**
   * The result FEAC buffer
   * @param buffer input buffer as a map timestamp of the pane -> IndexedSeq of (schema, distance, encoding schema)
   */
  class Buffer(private var buffer: Map[Long, IndexedSeq[(SchemaWithTimestamp, Option[Double], Option[Int])]]) {
    def this() = this(Map())
    def maintainFrom(window: Window) : Unit = {
      buffer = buffer.filterKeys(_ >= window.start.getTime)
    }
    def add(schema: SchemaWithTimestamp, paneTime: Long): Unit = {
      buffer += (paneTime -> (buffer.getOrElse(paneTime, IndexedSeq()) :+ (schema, None, None)))
    }
    def add(schema: SchemaWithTimestamp, distance: Double, clusterIndex: Int, paneTime: Long): Unit = {
      buffer += (paneTime -> (buffer.getOrElse(paneTime, IndexedSeq()) :+ (schema, Some(distance), Some(clusterIndex))))
    }
    def isEmpty: Boolean = buffer.isEmpty
    def size: Int = buffer.map(_._2.size).sum

    def data: Map[Long, IndexedSeq[(SchemaWithTimestamp, Option[Double], Option[Int])]] = buffer
    def schemas: Map[Long, IndexedSeq[SchemaWithTimestamp]] = buffer.map{
      case (k, v) => k -> v.map(_._1)
    }
  }

  /**
   *
   * @param clusters            input clustering result
   * @param element             element to insert in the cluster
   * @param t                   actual time
   * @param initSize            size for start clustering
   * @param PHTester            the tester for PH status
   * @param buffer              the buffer of warnings data, with timestamp
   * @param numberOfPopulations number of populations for FEAC algorithm, indicate how much replicate copies of individuals are needed for generate for a population
   * @param iter                number of k-means iterations in FEAC
   * @param elementsForMutation number of elements to apply mutation, is a function for FEAC that varies w.r.t. the number of individuals
   * @param actualWindow        the actual window of data, for remove not valid clusters
   * @param startingK starting number of clusters (k), optional -- for test purpose
   * @param maxK max number of clusters (k), optional -- for test purpose
   * @param saveRawData         save raw data of window elements
   * @param randomInit          random initialization of clusters
   * @param monitoringStats     statistic monitored
   * @return the updated clusters if possible and the updated warning buffer, the monitored statistics
   */
  def insertion(clusters: FeacStreamClusteringResult, element: SchemaWithTimestamp, t: Long, initSize: Int,
                PHTester: PageHinkleyTest, buffer: Buffer, numberOfPopulations: Int, iter: Int,
                elementsForMutation: Int => Int, actualWindow: Window, startingK: Option[Int], maxK: Option[Int],
                saveRawData: Boolean, randomInit: Boolean, monitoringStats: MonitoringStats): (FeacStreamClusteringResult, Buffer, MonitoringStats) = {
    var monitoringStatistics: MonitoringStats = monitoringStats
    LOGGER.debug(s"Start insert $element")
    if (buffer.isEmpty && !clusters.isEmpty) {
      LOGGER.debug("Try to update the result")
      var time = System.currentTimeMillis()
      val (newClusters, distance, clusterIndex) = update(clusters, element, t, actualWindow, saveRawData)
      monitoringStatistics = monitoringStatistics.updateValue(MonitoringProperty.TIME_FOR_UPDATE_RESULT, System.currentTimeMillis() - time)
      LOGGER.debug("End update the result")
      time = System.currentTimeMillis()
      val state = PHTester.compute(distance)
      monitoringStatistics = monitoringStatistics.updateValue(MonitoringProperty.TIME_FOR_TEST_RESULT, System.currentTimeMillis() - time)
      isInAlarm = state == ALARM_STATE
      if (state != STATIONARY_STATE) { //warning state
        LOGGER.info(s"Move to $state")
        val newBuffer = new Buffer()
        time = System.currentTimeMillis()
        newBuffer.add(element, distance, clusterIndex, t)
        monitoringStatistics = monitoringStatistics.updateValue(MonitoringProperty.TIME_FOR_UPDATE_DATA_STRUCTURE, System.currentTimeMillis() - time)
        (newClusters, newBuffer, monitoringStatistics)
      } else {
        LOGGER.debug(s"Add $element to the nearest cluster")
        (newClusters, new Buffer(), monitoringStatistics)
      }
    } else {
      if (clusters.isEmpty) {
        LOGGER.debug("Put element in the buffer")
        isInAlarm = false
        var time = System.currentTimeMillis()
        buffer.maintainFrom(actualWindow)
        buffer.add(element, t)
        monitoringStatistics = monitoringStatistics.updateValue(MonitoringProperty.TIME_FOR_UPDATE_DATA_STRUCTURE, System.currentTimeMillis() - time)
        if (buffer.size >= initSize) {
          LOGGER.info(s"Start generate first cluster generation")
          time = System.currentTimeMillis()
          val newClusters = firstCluster(buffer.schemas, numberOfPopulations, KMeansOnSchemas.euclideanSpace, saveRawData, PHTester, t, startingK, maxK, randomInit)
          monitoringStatistics = monitoringStatistics.updateValue(MonitoringProperty.TIME_FOR_CLUSTERING, System.currentTimeMillis() - time)
          LOGGER.debug(s"Generate first cluster generation")
          (newClusters, new Buffer(), monitoringStatistics)
        } else {
          LOGGER.debug(s"Put the element in the buffer, not enough elements ($initSize) for start clustering")
          (clusters, buffer, monitoringStatistics)
        }
      } else {
        LOGGER.debug("Update the clusters and monitor the situation")
        var time = System.currentTimeMillis()
        val (newClusters, distance, clusterIndex) = update(clusters, element, t, actualWindow, saveRawData)
        monitoringStatistics = monitoringStatistics.updateValue(MonitoringProperty.TIME_FOR_UPDATE_RESULT, System.currentTimeMillis() - time)
        time = System.currentTimeMillis()
        buffer.maintainFrom(actualWindow)
        buffer.add(element, distance, clusterIndex, t)
        monitoringStatistics = monitoringStatistics.updateValue(MonitoringProperty.TIME_FOR_UPDATE_DATA_STRUCTURE, System.currentTimeMillis() - time)
        LOGGER.debug("End update the result")
        if (isInAlarm && buffer.size >= initSize) { // alarm state && newBuffer.size >= BUFFER_MIN_SIZE // initSize * 0.1
          time = System.currentTimeMillis()
          LOGGER.info("Enter in alarm state")
          //here we need to obtain the actual encoding scheme of the buffer
          LOGGER.debug(s"New buffer size = ${buffer.size}")
          //val e = actualEncodingScheme
          // fix numbers if for example 1 is not present, etc., in this case fixe also the index
          //val encodingCluster = e.distinct.map(_.toLong).zipWithIndex.toMap
          //actualEncodingScheme = e.map(e => encodingCluster(e))
          //get actual clusters
          val vectorSpace = KMeansOnSchemas.euclideanSpace
          val c : ClusteringResult = buffer.data.flatMap {
            case (_, schemas) => schemas.map{
              case (s, Some(d), Some(i)) if newClusters.clusters.contains(i) => (i, (s.asInstanceOf[Schema], d))
            }
          }.groupBy(_._1).map {
            case (i, tuples) => newClusters.clusters(i).centroidSchema -> tuples.values.toSeq
          }
          LOGGER.info(s"Start generate new cluster generation")
          val time2 = System.currentTimeMillis()
          val reclusteredClusters = cluster(c, buffer.schemas, numberOfPopulations, iter, elementsForMutation, vectorSpace, maxK, saveRawData, PHTester, t)
          LOGGER.debug(s"Generate new cluster generation")
          isInAlarm = false
          monitoringStatistics = monitoringStatistics.updateValue(MonitoringProperty.TIME_FOR_WHOLE_RECLUSTERING, System.currentTimeMillis() - time)
            .updateValue(MonitoringProperty.NUMBER_OF_RECLUSTERING, 1)
            .updateValue(MonitoringProperty.TIME_FOR_CLUSTERING, System.currentTimeMillis() - time2)
          (reclusteredClusters, new Buffer(), monitoringStatistics)
        } else if(!isInAlarm) {
          val time = System.currentTimeMillis()
          val PHTesterResult = PHTester.compute(distance)
          monitoringStatistics = monitoringStatistics.updateValue(MonitoringProperty.TIME_FOR_TEST_RESULT, System.currentTimeMillis() - time)
          if(PHTesterResult == STATIONARY_STATE) (newClusters, new Buffer(), monitoringStatistics) else {
            if(PHTesterResult == ALARM_STATE) {
              isInAlarm = true
            }
            (newClusters, buffer, monitoringStatistics)
          }
        } else {
          (newClusters, buffer, monitoringStatistics)
        }
      }
    }
  }

  /**
   *
   * @param clusters       input clusters
   * @param element        new element
   * @param t              element timestamp
   * @param saveRawData    save raw data of window elements
   * @param actualWindow   actual window period
   * @return updated clusters adding element w.r.t. Euclidean distance and return the distance from the cluster
   */
  private def update(clusters: FeacStreamClusteringResult, element: SchemaWithTimestamp, t: Long, actualWindow: Window, saveRawData: Boolean):
    (FeacStreamClusteringResult, Double, Int) = {
    //look for a candidate cluster and add into it
    if (clusters.isEmpty) {
      (FeacStreamClusteringResult(
        Map(0 -> new FeacStreamFeatureVector(0, element.timestamp.getTime, Seq(element))),
        Map(t -> IndexedSeq(0)),
        if(saveRawData) Map(t -> IndexedSeq(element)) else Map()
      ), 0D, 0)
    } else {
      val (candidateCluster, (clusterDistance, clusterIndex)) = clusters.clusters.values.map {
        c =>
          val centroid = c.centroidSchema
          c -> (centroid.euclideanDistance(element), c.index)
      }.minBy(_._2._1)
      LOGGER.debug(s"The candidate cluster for ${element.value} is cluster with centroid ${candidateCluster.centroidSchema.value}")
      clusters.add(candidateCluster, element, t, saveRawData)
      (clusters, clusterDistance, clusterIndex)
    }
  }

  /**
   *
   * @param actualClusters input clusters
   * @param actualWindow the actual window
   * @param saveRawData true if save raw data
   * @return valid cluster
   */
  def removeOldestClusteringResult(actualClusters: FeacStreamClusteringResult, actualWindow: Window, saveRawData: Boolean): FeacStreamClusteringResult = {
    val validClusters = actualClusters.clusters.filter(c => actualWindow.contains(c._2.t))
    val validEncodingSchema = actualClusters.encodingScheme.filterKeys(_ >= actualWindow.start.getTime)

    if (validClusters.isEmpty) FeacStreamClusteringResult() else {
      if(saveRawData) {
        val validSchemas = actualClusters.schemas.filterKeys(_ >= actualWindow.start.getTime)
        FeacStreamClusteringResult(
          validClusters,
          validEncodingSchema,
          validSchemas
        )
      } else {
        // it can't be done but this line is never called because we always do saveRawData
        FeacStreamClusteringResult(validClusters, validEncodingSchema, Map())
      }
    }
  }

  /**
   *
   * @param inputSchemaMap      input schemas with sorted in the buffer
   * @param numberOfPopulations number of populations for FEAC algorithm, indicate how much replicate copies of individuals are needed for generate for a population
   * @param vectorSpace         the used vector space
   * @param saveRawData         save raw data of window elements
   * @param PHTester            the ph tester, use to update the distance from the centroid
   * @param operationTime       is the time when the cluster operation is called
   * @param startingK starting number of clusters (k), optional -- for test purpose
   * @param maxK max number of clusters (k), optional -- for test purpose
   * @param randomInit if true, the initial centroids are random, otherwise are the first k elements of the dataset
   * @return FEAC clustering of schemas, firstCluster of the buffer
   */
  private def firstCluster(inputSchemaMap: Map[Long, IndexedSeq[SchemaWithTimestamp]], numberOfPopulations: Int, vectorSpace: VectorSpace[Schema],
                           saveRawData: Boolean, PHTester: PageHinkleyTest, operationTime: Long, startingK: Option[Int], maxK: Option[Int], randomInit: Boolean): FeacStreamClusteringResult = {
    val schemas = inputSchemaMap.flatMap(_._2).toSeq
    val ks = if(startingK.isDefined) Seq(startingK.get) else 2 to maxK.getOrElse(math.sqrt(schemas.size).intValue())
    val sortedSchemas = schemas.sortBy(_.timestamp.getTime) // could be eliminated
    //extract numberOfPopulations time ks values, for generate starting population
    val population: IndexedSeq[(ClusteringResult, SSResult[Schema])] = (1 to numberOfPopulations) map {
      i =>
        val k = pickRandom(ks, 1, !saveRawData, i).head
        val centroids = if(startingK.isDefined) kMeansPP(sortedSchemas.map(x => x.asInstanceOf[Schema]), k, randomInit = randomInit)(vectorSpace, toCenter = x => x) else pickRandom(sortedSchemas, k, !saveRawData)
        val r = createClusterFromCentroids(centroids, schemas, vectorSpace)
        r -> simplifiedSilhouette(r, vectorSpace)
    }
    val cluster = population.maxBy(_._2._2)._1
    val clusters = fromClusteringResultToFeacResult(saveRawData, cluster, inputSchemaMap)
    PHTester.initParameters(clusters, cluster, operationTime)
    clusters
  }

  /**
   * @param clusteringResult    actual clustering result
   * @param inputSchemaMap             input schemas with timestamp, sorted as input buffer
   * @param numberOfPopulations number of populations for FEAC algorithm, indicate how much replicate copies of individuals are needed for generate for a population
   * @param iter                number of k-means iterations in FEAC
   * @param elementsForMutation number of elements to apply mutation, is a function for FEAC that varies w.r.t. the number of individual
   * @param vectorSpace         the used vector space
   * @param maxK max number of clusters (k), optional -- for test purpose
   * @param saveRawData         save raw data of window elements
   * @param PHTester            the ph tester, use to update the distance from the centroid
   * @param operationTime       is the time when the cluster operation is called
   * @return FEAC clustering of schemas, using clusters as starting partition
   */
  private def cluster(clusteringResult: ClusteringResult, inputSchemaMap: Map[Long, IndexedSeq[SchemaWithTimestamp]], numberOfPopulations: Int, iter: Int,
                      elementsForMutation: Int => Int, vectorSpace: VectorSpace[Schema], maxK: Option[Int], saveRawData: Boolean,
                      PHTester: PageHinkleyTest, operationTime: Long): FeacStreamClusteringResult = {
    val ss = simplifiedSilhouette(clusteringResult, vectorSpace)
    val population = (1 to numberOfPopulations).map(_ => clusteringResult -> ss)
    cluster(population, inputSchemaMap, iter, elementsForMutation, vectorSpace, maxK, saveRawData, PHTester, operationTime)
  }

  /**
   * @param individualIndex   the individual index
   * @param generation        the generation number
   * @param mutationOperation the selected mutation operation {1, 2}
   * @param individual        the individual where apply the mutation operation
   * @param vectorSpace       the used vector space
   * @param maxK max number of clusters (k), optional -- for test purpose
   * @param saveRawData       save raw data of window elements
   * @return the individual with applied the mutation operation
   */
  private def applyMutationOperation(individualIndex: Int, generation: Int, mutationOperation: ExecutedMutationOperation, individual: FeacIndividual, vectorSpace: VectorSpace[Schema], maxK: Option[Int], saveRawData: Boolean): ClusteringResult = {
    /**
     *
     * @param individual   individual
     * @param individualSS SS of clusters inside the individual
     * @param iteration the iteration of the z, for random seed
     * @return the random extraction of a cluster from the individual, considering SS in descending order
     */
    def mutationRouletteWheelExtraction(individual: ClusteringResult, individualSS: Map[Schema, Double], iteration: Int): (Schema, Seq[(Schema, Double)]) = {
      RouletteWheel.pickWeighted(individual.filterKeys(individualSS.keySet.contains(_)).map {
        case (schema, schemaToDouble) =>
          val ss = individualSS(schema)
          (schema, schemaToDouble) -> (if(ss.isNaN) 0 else ss)
      }.toSeq, 1, Ordering.Double.reverse, !saveRawData, individualIndex * generation * iteration).head
    }

    mutationOperation match {
      case MO1 =>
        if (individual.clustering.size > 2) {
          val z = pickRandom(1 to individual.clustering.size - 2, 1, !saveRawData, individualIndex * generation).head
          var newIndividual = individual.clustering
          val individualSS = individual.ss._1
          LOGGER.debug(s"MO1 - Extracted z = $z")
          for (it <- 1 to z) {
            val clusterToRemove = mutationRouletteWheelExtraction(newIndividual, individualSS, it)
            LOGGER.debug(s"MO1 - Remove ${clusterToRemove._1}")
            newIndividual = newIndividual.filter(_ != clusterToRemove)
            for (element <- clusterToRemove._2.map(_._1)) {
              //assign element to the closest cluster
              val clusterToAssign = newIndividual.keys.map(c => c -> vectorSpace.distance(c, element)).minBy(_._2)
              newIndividual += (clusterToAssign._1 -> (newIndividual(clusterToAssign._1) :+ (element -> clusterToAssign._2)))
            }
          }
          newIndividual
        } else individual.clustering
      case MO2 =>
        if(maxK.isDefined && individual.clustering.size >= maxK.get) individual.clustering else {
          val z = pickRandom(1 to individual.clustering.size, 1, !saveRawData, individualIndex * generation).head
          LOGGER.debug(s"MO2 - Extracted z = $z")
          var newIndividual = individual.clustering
          val individualSS = individual.ss._1
          for (it <- 1 to z) {
            val clusterToSplit = mutationRouletteWheelExtraction(newIndividual, individualSS, it)
            if (clusterToSplit._2.size >= 2) {
              newIndividual = newIndividual.filter(_ != clusterToSplit)
              LOGGER.debug(s"MO2 - Split ${clusterToSplit._1}")
              val clusterElements = clusterToSplit._2.sortBy(_._2).map(_._1)
              val randomObjectS = pickRandom(clusterElements, 1, !saveRawData, individualIndex * generation * it).head
              val randomObjectJ = clusterElements.filter(_ != randomObjectS).map(e => e -> vectorSpace.distance(randomObjectS, e)).maxBy(_._2)._1
              newIndividual ++= createClusterFromCentroids(Seq(randomObjectS, randomObjectJ), clusterElements, vectorSpace)
            }
          }
          newIndividual
        }
    }
  }

  /**
   *
   * @param centroids   of a clustering solution
   * @param data        to clusterize and assign to centroids
   * @param vectorSpace used vector space
   * @return The clustering result, assigning each data to the nearest centroid
   */
  private def createClusterFromCentroids(centroids: Seq[Schema], data: Seq[Schema], vectorSpace: VectorSpace[Schema]): ClusteringResult = {
    var clusteringResult = centroids.map(c => c -> Seq[Schema]()).toMap
    for (e <- data) {
      val c = centroids.map(c => c -> vectorSpace.distance(e, c)).minBy(_._2)._1
      clusteringResult += c -> (clusteringResult(c) :+ e)
    }
    ResultManipulation.groupDataAndComputeCentroids(clusteringResult.values.toSeq, vectorSpace)
  }

  /**
   * @param actualPopulation    clusters population input of FEAC
   * @param inputSchemaMap             input schemas with timestamp, sorted as input buffer
   * @param iter                number of k-means iterations in FEAC
   * @param elementsForMutation number of elements to apply mutation, is a function for FEAC that varies w.r.t. the number of individuals
   * @param vectorSpace         the used vector space
   * @param maxK max number of clusters (k), optional -- for test purpose
   * @param saveRawData         save raw data of window elements
   * @param PHTester            the ph tester, use to update the distance from the centroid
   * @param operationTime       is the time when the cluster operation is called
   * @return FEAC clustering of schemas, using clusters as starting partition
   */
  private def cluster(actualPopulation: IndexedSeq[(ClusteringResult, SSResult[Schema])], inputSchemaMap: Map[Long, IndexedSeq[SchemaWithTimestamp]],
                      iter: Int, elementsForMutation: Int => Int, vectorSpace: VectorSpace[Schema], maxK: Option[Int],
                      saveRawData: Boolean, PHTester: PageHinkleyTest, operationTime: Long): FeacStreamClusteringResult = {
    val schemas = inputSchemaMap.flatMap(_._2).toSeq
    var gen = 1
    var population: Map[Int, Map[Int, FeacIndividual]] = Map(0 -> actualPopulation.zipWithIndex.map {
      case ((individual, simplifiedSilhouette), i) => i -> FeacIndividual(i, individual, simplifiedSilhouette, NoMutation, i, gen)
    }.toMap)
    population += gen -> population(0)
    val mutationOperations = Seq(MO1, MO2)
    /*
    We programmed FEAC-stream to stop when one of the following criteria (SC in Algorithm 1) was satisfied:
      - (i) if the fitness of the best individual does not improve more than 0.001 after ten consecutive generations;
      - or (ii) 50 generations have been completed.
     */
    var bestIndividualIndex: Option[Int] = None
    var fitnessSmallImprovementCount = 0
    while (gen <= MAX_NUMBER_OF_GENERATIONS && fitnessSmallImprovementCount < NUMBER_OF_SMALL_FITNESS_IMPROVEMENT) {
      LOGGER.info(s"Generation $gen")
      population += gen -> population(gen).map {
        case (index, individual) =>
          val r = kmeans(schemas, individual.clustering.size, vectorSpace, individual.clustering.keys.toSeq, iter)
          val ss = simplifiedSilhouette(r, vectorSpace)
          index -> FeacIndividual(index, r, ss, NoMutation, individual.actualIndex, gen)
      }
      val bestIndividual = population(gen).maxBy(_._2.ss._2)
      bestIndividualIndex match {
        case Some(i) if i == bestIndividual._2.pastGenerationIndex =>
          if (bestIndividual._2.ss._2 - population(gen)(bestIndividual._2.pastGenerationIndex).ss._2 <= SMALL_FITNESS_IMPROVEMENT) {
            fitnessSmallImprovementCount += 1
          } else {
            fitnessSmallImprovementCount = 0
          }
        case _ =>
          fitnessSmallImprovementCount = 0
          bestIndividualIndex = Some(bestIndividual._1)
      }

      val othersIndividuals: Seq[(Int, FeacIndividual)] = RouletteWheel.pickWeighted(population(gen).filterKeys(_ != bestIndividual._1).toSeq.map {
        case (i, individual) => (i, individual) -> individual.ss._2
      }, math.min(elementsForMutation(population(gen).size), population(gen).size - 1), Ordering.Double, !saveRawData)

      var nextGenPopulation = Map[Int, FeacIndividual]()
      for (individual <- othersIndividuals :+ bestIndividual) {
        LOGGER.debug(s"Individual ss = ${individual._2.ss._2} (id = ${individual._1})")
        val mutationOperation = if (gen <= 1) pickRandom(mutationOperations, 1, !saveRawData, individual._1).head else {
          //choose the best operation showing history
          val l1 = population(gen - 1)(individual._2.pastGenerationIndex)
          val l2 = population(gen - 2)(l1.pastGenerationIndex)
          if (l1.ss._2 > l2.ss._2) {
            l1.mutationOperation
          } else {
            mutationOperations.filter(_ != l1.mutationOperation).head
          }
        }
        val mutedIndividual = applyMutationOperation(individual._1, gen, mutationOperation.asInstanceOf[ExecutedMutationOperation], individual._2, vectorSpace, maxK, saveRawData)
        val newIndividual = FeacIndividual(individual._1, mutedIndividual, simplifiedSilhouette(mutedIndividual, vectorSpace), mutationOperation, individual._2.pastGenerationIndex, gen)
        nextGenPopulation += individual._1 -> newIndividual
        LOGGER.info(s"Generation $gen - Individual ${individual._1} - MO $mutationOperation - ss = ${newIndividual.ss._2} (old ss = ${population(gen -1)(newIndividual.pastGenerationIndex).ss._2})")
      }
      population += gen -> nextGenPopulation
      //copy individuals to the next population
      gen = gen + 1
      population += gen -> nextGenPopulation
    }
    val bestResult = population(gen).minBy(_._2.ss._2)._2.clustering
    val clusters = fromClusteringResultToFeacResult(saveRawData, bestResult, inputSchemaMap)
    PHTester.initParameters(clusters, bestResult, operationTime)
    clusters
  }

  private def fromClusteringResultToFeacResult(saveRawData: Boolean, clustering: ClusteringResult, inputSchemaMap: Map[Long, IndexedSeq[SchemaWithTimestamp]]) = {
    var clusters: Map[Int, FeacStreamFeatureVector] = Map()
    var schemasAssignments: Map[SchemaWithTimestamp, Int] = Map()
    var i = 0
    clustering.foreach {
      case (_, values) =>
        if (values.nonEmpty) {
          val schemas = values.map(_._1)
          val fv = new FeacStreamFeatureVector(i, schemas.map(_.asInstanceOf[SchemaWithTimestamp].timestamp.getTime).max, schemas)
          schemas.foreach(s => schemasAssignments += (s.asInstanceOf[SchemaWithTimestamp] -> i))
          clusters += i -> fv
          i = i + 1
        }
    }
    val encodeSchemas = inputSchemaMap.map(s => s._1 -> s._2.map(s => schemasAssignments(s)))
    new FeacStreamClusteringResult(clusters, encodeSchemas, if (saveRawData) inputSchemaMap else Map())
  }

  /**
   * A version of kmeans without type transform
   *
   * @param records           the input space record
   * @param k                 the k of kmeans input
   * @param vectorSpace       the vector space
   * @param startingCentroids starting k_means centroids
   * @param maxIter           max number of iterations admitted (default 100)
   * @tparam T the vector space type
   * @return the map of each cluster, with the centroid and the map of the values of each cluster
   *         within the distance from the centroid
   */
  private def kmeans[T](records: Seq[T], k: Int, vectorSpace: VectorSpace[T], startingCentroids: Seq[T], maxIter: Int): Map[T, Seq[(T, Double)]] = {
    val clusters = KMeans.cluster[T, T](records, k, startingCentroids, maxIter)((x: T) => x, vectorSpace)
    ResultManipulation.groupDataAndComputeCentroids(clusters, vectorSpace)
  }

}
