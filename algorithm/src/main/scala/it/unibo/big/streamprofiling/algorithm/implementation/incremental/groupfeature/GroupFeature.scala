package it.unibo.big.streamprofiling.algorithm.implementation.incremental.groupfeature

import it.unibo.big.streamprofiling.algorithm.utils.MapUtils._
import it.unibo.big.utils.input.SchemaModeling.{BasicSchema, Schema}

/**
 *
 * @param timestamp the last timestamp
 * @param schemasTmp schemas in the group feature
 */
private [incremental] abstract class GroupFeature(timestamp: Long, private var schemasTmp: Seq[Schema]) {
  import scala.runtime.ScalaRunTime

  /**
   * Initial dimension of the group feature
   */
  def initialSize: Int

  /**
   * Initial Linear Sum of the group feature
   */
  def startingLS : Map[String, Double]

  /**
   * Initial Sum of Square of the group feature
   */
  def startingSS : Map[String, Double]

  private var numberOfElements: Int = initialSize
  require(startingSS.keySet.equals(startingLS.keySet))
  private var linearSum: Map[String, Double] = startingLS
  private var sumOfSquare: Map[String, Double] = startingSS
  private var lastTimeStamp = timestamp

  /**
   *
   * @return most used schema in the gf
   */
  def seedName: String = schemas.map(s => s.seedName -> 1).groupBy(_._1).maxBy(_._2.size)._1

  /**
   *
   * @return schemas that are active in the group feature, for debug the result
   */
  def schemas: Seq[Schema] = schemasTmp

  /**
   * @return number of element of the group feature
   */
  def N: Int = numberOfElements

  /**
   * @return linear sum (LS) of the group feature
   */
  def LS: Map[String, Double] = linearSum

  /**
   * @return sum of square (SS) of the group feature
   */
  def SS: Map[String, Double] = sumOfSquare

  /**
   * @return the most recent timestamp of the group feature
   */
  def t: Long = lastTimeStamp

  /**
   * @return the centroid of the group feature
   */
  def centroid: Map[String, Double] = divide(LS, N)

  /**
   * @return the centroid using schema dictionary
   */
  def centroidSchema: Schema = BasicSchema(centroid)

  /**
   * @param s the schema to add
   * @param t schema timestamp
   *          Update the group feature adding s
   */
  def add(s: Schema, t: Long): Unit = {
    numberOfElements += 1
    linearSum = sumDouble(LS, s.values)
    sumOfSquare = sumDouble(LS, square(s.values))
    lastTimeStamp = math.max(t, lastTimeStamp)
    schemasTmp :+= s
  }

  /**
   * @param cf the group feature to add
   *           Update the group feature adding cf, updating also the schemas
   */
  def add[T <: GroupFeature](cf: GroupFeature): T = {
    schemasTmp ++= cf.schemas
    addWithoutSchemas(cf)
  }

  /**
   *
   * @param cf the group feature to add
   *           Update the group feature adding cf
   * @tparam T the type to return
   * @return update the feature without adding raw schemas
   */
  def addWithoutSchemas[T <: GroupFeature](cf: GroupFeature): T = {
    linearSum = sumDouble(LS, cf.LS)
    sumOfSquare = sumDouble(SS, cf.SS)
    numberOfElements += cf.N
    lastTimeStamp = math.max(lastTimeStamp, cf.t)
    this.asInstanceOf[T]
  }

  override def equals(obj: Any): Boolean = obj match {
    case x: GroupFeature if x.LS == LS && x.SS == SS && x.N == N && x.t == t => true
    case _ => false
  }

  override def hashCode(): Int = ScalaRunTime._hashCode((LS, SS, N, t))

  /**
   *
   * @return cluster radius Without divisor (for FEAC) (defined by A Framework for Projected Clustering of High Dimensional Data Streams - Aggarwal 2004)
   */
  def clusterRadiusFeac: Double =
    math.sqrt(LS.keySet.map(i => (SS(i) / N) - (math.pow(LS(i), 2) / math.pow(N, 2))).sum) // in Aggarwal the argument of sqrt is: / LS                             .size)

}

