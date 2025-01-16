package it.unibo.big.streamprofiling.algorithm.utils

/**
 * Json utilities class
 */
object JsonUtils {
  import it.unibo.big.utils.input.SchemaModeling.ClusteringResult
  import org.json4s.JsonDSL._
  import org.json4s._

  /**
   *
   * @param cluster the identifying name of the used clustering
   * @param k the k input of kmeans algorithm
   * @return the json representation of the clustering result according to the given parameter
   */
  def clustersWriter(cluster: String, k: Int): Writer[ClusteringResult] = new Writer[ClusteringResult] {
    override def write ( m: ClusteringResult ): JValue = {
      new JArray (m.map{
        case (centroid, values) =>
          //JObject("k" -> JInt(k)) ~
          JObject(cluster -> JString(centroid.value)) ~
            //JObject("values" -> new JArray(values.toList.map{ case (v, dist) => JObject(s"${v.seedName}-${v.value}" -> JDouble(dist)) }))
            JObject("values" -> JInt(values.size)) ~
            JObject("avg(distance)" -> JDouble(values.map(_._2).sum / values.size))
      }.toList)
    }
  }
}