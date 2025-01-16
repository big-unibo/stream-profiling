package it.unibo.big.streamprofiling.algorithm.implementation.kmeans

import it.unibo.big.streamprofiling.algorithm.utils.VectorSpace.VectorSpace

/**
 * KMeans algorithm
 */
object KMeans {
  import it.unibo.big.streamprofiling.algorithm.utils.RandomUtils.pickRandom
  import org.slf4j.{Logger, LoggerFactory}

  import scala.annotation.tailrec

  private val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)


  /**
   *
   * @param xs an input sequence
   * @param k the number of elements to pick
   * @param space the vector space
   * @param toCenter a function that map T elements to U
   * @param randomInit of the first element is random or not
   * @tparam U the type of the elements
   * @return the kmeans++ init of values
   */
  def kMeansPP[U](xs: Seq[U], k: Int, randomInit: Boolean)(space: VectorSpace[U], toCenter: U => U): Seq[U] = {
    case class Pair(original: U) {
      val center: U = toCenter(original)
    }
    val associated = xs.map(Pair)
    var centroids = (if(randomInit) pickRandom(associated, 1, random = randomInit) else Seq(associated.head)).map(_.center) //take the first element for avoid randomness of parallel executions pickRandom(associated, 1, random = false).map(_.center)
    LOGGER.debug(s"Centroid for $k = ${centroids.head}")
    for (_ <- 1 until k) {
      val nextCentroid = util.Try(associated.filter(x => !centroids.contains(x.center))
        .map(r => centroids.map(c => r -> space.distance(c, r.original)).minBy(_._2)).maxBy(_._2)._1).toOption
      if(nextCentroid.isDefined) {
        centroids :+= nextCentroid.get.center
      }
    }
    centroids
  }

  /**
   *
   * @param xs the input vector elements sequence
   * @param k the k value for k means
   * @param projection the function for move elements from T to U
   * @param space the vector space
   * @param startingCentroids starting centroids of cluster
   * @param maxIter max number of iterations admitted
   * @tparam T the input type
   * @tparam U the vector space type
   * @return a sequence of elements, each inner sequence is a cluster
   */
  def cluster[T,U](xs: Seq[T], k: Int, startingCentroids: Seq[U], maxIter: Int)
                  (implicit projection: T => U, space: VectorSpace[U]): Seq[Seq[T]] = {
    case class Pair(original: T) {
      val projected: U = projection(original)
    }
    var it = 0
    @tailrec
    def step(xs: Seq[Pair], centroids: Seq[U], lastResult: Seq[Seq[Pair]] = Seq()): Seq[Seq[Pair]] = {
      it += 1 //increment the number of iterations

      val labeled =
        for (x <- xs) yield {
          val distances = for (centroid <- centroids) yield (centroid, space.distance(centroid, x.projected))
          val nearestCentroid = distances.minBy(_._2)._1
          (nearestCentroid, x)
        }
      val grouped = for (centroid <- centroids) yield labeled.collect({
        case (`centroid`, x) => x
      })
      val replacements = grouped.map(group => space.centroid(group.map(_.projected)))
      val stable =
        replacements.forall {
          replacement =>
            centroids.contains(replacement)
        }
      val sameClusters = if(lastResult.isEmpty) false else {
        lastResult.forall(grouped.contains) && lastResult.size == grouped.size
      }
      LOGGER.debug(s"KMeans iter = $it (max iter = $maxIter) stable = $stable, same clusters = $sameClusters")
      if (stable || it >= maxIter || sameClusters) {
        grouped
      } else {
        step(xs, replacements, grouped)
      }
    }
    val associated = xs.map(Pair)
    step(associated, startingCentroids).map(_.map(_.original))
  }

  /**
   *
   * @param xs         the input vector elements sequence
   * @param k          the k value for k means
   * @param projection the function for move elements from T to U
   * @param space      the vector space
   * @param randomInit true if random init
   * @param kMeansPPInit true if random init, false if kmeans++ (default)
   * @param maxIter    max number of iterations admitted
   * @param toCenter   a function that map an element to a center
   * @tparam T the input type
   * @tparam U the vector space type
   * @return a sequence of elements, each inner sequence is a cluster
   */
  def cluster[T, U](xs: Seq[T], k: Int, randomInit: Boolean, maxIter: Int, kMeansPPInit: Boolean)
                   (implicit projection: T => U, space: VectorSpace[U], toCenter: U => U): Seq[Seq[T]] = {
    case class Pair(original: T) {
      val projected: U = projection(original)
    }
    val associated = xs.map(Pair)
    val initial = if (!kMeansPPInit) pickRandom(associated.map(_.projected), k, random = randomInit) else kMeansPP[U](associated.map(_.projected), k, randomInit = randomInit)(space, toCenter)
    cluster(xs, k, initial, maxIter)
  }

  /**
   * A carried simplification with implicit vector space of the above function
   */
  def cluster[T,U](fn: T => U)(xs: Seq[T], k: Int, randomInit: Boolean, maxIter: Int, kMeansPP: Boolean)
                  (implicit g: VectorSpace[U], toCenter: U => U): Seq[Seq[T]] = cluster(xs, k, randomInit, maxIter: Int, kMeansPP)(fn, g, toCenter)

  /**
   * A version of kmeans without type transform
   * @param records the input space record
   * @param k the k of kmeans input
   * @param vectorSpace the vector space
   * @param randomInit true if you use randomness in the init
   * @param kMeansPP true if random init, false if kmeans++ (default)
   * @param maxIter max number of iterations admitted (default 100)
   * @param toCenter   a function that map an element to a center, default identity
   * @tparam T the vector space type
   * @return the map of each cluster, with the centroid and the map of the values of each cluster
   *         within the distance from the centroid
   */
  def kmeans[T](records: Seq[T], k: Int, vectorSpace: VectorSpace[T], randomInit: Boolean = true, kMeansPP: Boolean = true, maxIter: Int = 100, toCenter: T => T = (x: T) => x): Map[T, Seq[(T, Double)]] = {
    val clusters = cluster[T, T](fn = (x : T) => x )(records, k, randomInit, maxIter, kMeansPP)(vectorSpace, toCenter)
    ResultManipulation.groupDataAndComputeCentroids(clusters, vectorSpace)
  }

  /**
   * Object for manipulate clustering results
   */
  object ResultManipulation {

    /**
     *
     * @param clusters    the result of clustering
     * @param vectorSpace the vector space
     * @tparam T the data type of clustering
     * @return the result of clustering in form of map of clusters -> with each cluster centroids has a map of values and distances
     */
    def groupDataAndComputeCentroids[T](clusters: Seq[Seq[T]], vectorSpace: VectorSpace[T]): Map[T, Seq[(T, Double)]] =
      clusters.map(xs => {
        val centroid = vectorSpace.centroid(xs)
        centroid -> xs.distinct.map(x => x -> vectorSpace.distance(centroid, x)).toMap
      }).groupBy(_._1).map {
        case (centroid, values) => centroid -> values.flatMap(_._2)
      }
  }
}