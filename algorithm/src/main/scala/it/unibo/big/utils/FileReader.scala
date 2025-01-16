package it.unibo.big.utils

import java.nio.charset.CodingErrorAction
import scala.io.{BufferedSource, Codec}

/** module for import files with implicit conversion */
object FileReader {

  /**
   *
   * @param resourceName resource file from resource folder
   * @return the source of the read file
   */
  private def fromResource(resourceName: String): BufferedSource =
    new BufferedSource(getClass.getClassLoader.getResourceAsStream(resourceName))

  /**
   *
   * @param fileName file name
   * @param action the action used to parse de file
   * @param isResource true if file is a resource in project
   * @tparam T the returned type of the file content
   * @return the T representation of file rows
   */
  def fromFile[T](fileName: String, action: BufferedSource => T, isResource : Boolean = true): T = {
    val source = if (!isResource) scala.io.Source.fromFile ( fileName ) else fromResource(fileName)
    val res = action(source)
    source.close
    res
  }

  import org.slf4j.{Logger, LoggerFactory}
  private val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName)

  /**
   * Read a file applying its converter
   *
   * @param fileName name of resource file
   * @param header true if file has header
   * @tparam T file row class
   * @param isResource true if file is a resource in project
   * @return Seq of T rows in file
   */
  def readFile[T: MyConverter] ( fileName: String, header : Boolean = true, isResource : Boolean = true): Seq[T] = {
    try {
      LOGGER.info(s"Start read and parse file $fileName")
      val (bufferedSource, rows) = read(fileName, header, isResource)
      val rowsList = rows.toList
      bufferedSource.close
      LOGGER.info(s"End read file $fileName")
      rowsList
    } catch {
      case e: Exception =>
        e.printStackTrace()
        Seq[T]()
    }
  }

  /**
   * Reads a file in lazy mode, without closing buffered source, used for bulk inserts
   * @param fileName name of resource file
   * @param header true if file has header
   * @param isResource true if file is a resource in project
   * @tparam T  file row class
   * @return the iterator of the converted rows from the input file
   */
  def readLazyFile[T: MyConverter] ( fileName: String, header : Boolean = true, isResource : Boolean = true): Iterator[T] = {
    LOGGER.info ( s"Start read lazy and parse file $fileName" )
    val (_, rows) = read(fileName, header, isResource)
    LOGGER.info ( s"End read lazy file $fileName" )
    rows
  }

  /**
   * Utility method for read the file
   * @param fileName name of the file
   * @param header true if file has header
   * @param isResource true if file is a resource in project
   * @tparam T file row class, within a converter
   * @return the source and the iterator of the converted input file rows
   */
  private def read[T: MyConverter](fileName: String, header: Boolean, isResource: Boolean): (BufferedSource, Iterator[T]) = {
    // Specify UTF-8 charset and replace unmappable characters
    implicit val codec: Codec = Codec.UTF8.onUnmappableCharacter(CodingErrorAction.REPLACE)

    val bufferedSource = if (!isResource) scala.io.Source.fromFile ( fileName ) else fromResource ( fileName )
    var data = bufferedSource.getLines ()
    if (header) {
      //set the header to the converter
      implicitly [MyConverter[T]] header data.take(1).toSeq.headOption.getOrElse("")
      data = data.drop ( 1 )
    }
    val rows = data.map ( implicitly [MyConverter[T]] parse _ )
    (bufferedSource, rows)
  }

  /**
   * A trait for a converter of A data that parse the data
   * @tparam A return type of the converter
   */
  trait MyConverter[A] {
    def parse ( line: String ): A
    def header ( line: String ): Unit = {}
  }
}