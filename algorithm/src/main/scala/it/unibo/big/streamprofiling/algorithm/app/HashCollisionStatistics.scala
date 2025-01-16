package it.unibo.big.streamprofiling.algorithm.app

import it.unibo.big.streamprofiling.algorithm.execution.ClusteringExecution.readData
import it.unibo.big.streamprofiling.algorithm.utils.RandomUtils.RANDOM_SEED
import it.unibo.big.streamprofiling.algorithm.utils.Settings.CONFIG
import it.unibo.big.utils.FileWriter
import it.unibo.big.utils.input.SchemaModeling.Schema
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Random
import scala.util.hashing.MurmurHash3

/**
 * Debug class for checking the statistics of the hash collision
 */
object HashCollisionStatistics extends App {
  private val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)
  private val l = 15

  /**
   *
   * @param schema an input schema
   * @param nBits  the number of bits (hash function)
   * @return the attributes that have created the signature and the signature for the schema
   */
  private def createHash(schema: Schema, nBits: Int): (IndexedSeq[String], IndexedSeq[Int]) = {
    val res = (1 to nBits) map (i => schema.values.map(a => applyHash(a._1, i) -> a._1 ).min)
    (res.map(_._2), res.map(_._1))
  }

  /**
   *
   * @param attribute an attribute name
   * @param seed      the seed for the hash function
   * @return the hash value for the attribute within the seed
   */
  private def applyHash(attribute: String, seed: Int): Int = {
    Random.setSeed(RANDOM_SEED)
    MurmurHash3.stringHash(attribute, seed)
  }

  /**
   * Read data from a list of files and compute the statistics, writing them in a file specified in the conf "2phases.collisions" and "2phases.collisions2"
   * @param fileNames the file names
   */
  def statistics(fileNames: Seq[String]): Unit = {
    var data = Seq[Seq[Any]]()
    var data2 = Seq[Seq[Any]]()

    for(fileName <- fileNames) {
      LOGGER.info(s"Reading data from $fileName")
      val schemasSet: Set[Schema] = readData(fileName, readAsAStream = false).toSet
      // get hashes from schemas
      val hashes = schemasSet.map(s => createHash(s, l) -> s)
      hashes.groupBy(_._1._2).foreach {
        case (hash, xs) =>
          // find collisions
          val collisions = hash.indices.map(i => xs.map(_._1._1(i))).count(_.size > 1)
          val contributionAttributes = xs.map(_._1._1.toString)
          data ++= Seq(Seq[Any](hash.size, collisions, contributionAttributes.size, contributionAttributes.mkString("[]"), fileName))
      }

      (0 until l).foreach(i => {
        hashes.groupBy(_._1._2(i)).map {
          case (hash, xs) =>
            val collisions = xs.map(_._1._1(i))
            data2 ++= Seq(Seq[Any](i, hash, xs.size, collisions.size, collisions.mkString("[]"), fileName))
        }
      })
    }

    val fileName = s"${CONFIG.getString("2phases.collisions")}"
    FileWriter.writeFileWithHeader(data,
      Seq("hash_size", "collisions", "contributionAttributesSize", "contributionAttributes", "fileName"), fileName)

    val fileName2 = s"${CONFIG.getString("2phases.collisions2")}"
    FileWriter.writeFileWithHeader(data2,
      Seq("hash_index", "value", "sizeHashValueIndex", "contributionAttributesSize", "contributionAttributes", "fileName"), fileName2)
  }

  statistics(Seq("standard", "fadein", "fadeout", "split", "slide", "merge").map(s => s"stream-profiling/datasets/$s-common1-fixed0.9/seeds_SIM.csv"))
}
