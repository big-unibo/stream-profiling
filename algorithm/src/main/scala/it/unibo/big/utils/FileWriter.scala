package it.unibo.big.utils

import java.io.File

/** Helper for write files */
object FileWriter {

  /**
   * File writer
   * @param filename name of the file
   * @param writeFun the function for write contents in the file
   * @param synchronized a boolean file that if true tells to apply a synchronized writing
   */
  private def writeFile(filename: String, writeFun : java.io.File => Unit, synchronized: Boolean = false): Unit = {
    val file = new java.io.File ( filename )
    if (!file.exists ()) {
      file.getParentFile.mkdirs
      file.createNewFile
    }
    if(synchronized) {
      file.getCanonicalPath.intern().synchronized{
        writeFun(file)
      }
    } else {
      writeFun(file)
    }
  }

  import com.github.tototoshi.csv.CSVWriter

  /**
   * Appends contents to the csv file
   * @param file the input file
   * @param writeFun the write function
   */
  private def appendCsv(file: java.io.File, writeFun : CSVWriter => Unit): Unit = writeCsv(file, writeFun, append = true)

  /**
   * A csv writer.
   *
   * @param file the input file
   * @param writeFun the write function
   * @param append a boolean parameter that if true tells to append the content
   */
  def writeCsv ( file: java.io.File, writeFun : CSVWriter => Unit, append: Boolean = false): Unit = {
    val writer = CSVWriter.open(file, append = append)
    writeFun(writer)
    writer.close()
  }

  /**
   * Writes data in the file name, in overwrite mode
   *
   * @param fileName the file name
   * @param data the input data
   */
  def writeCsv(fileName: String, data: Seq[Seq[Any]]): Unit = writeFile(fileName, f => writeCsv(f, _.writeAll(data)))

  /**
   * Method for write synchronously and more than once a csv with an initial header
   *
   * @param data the data to write
   * @param header the csv header
   * @param fileName the csv file name
   * @param synchronized true if you want synchronized writing, default true
   * @param overwrite true if you want to overwrite the file, default false
   */
  def writeFileWithHeader(data: Seq[Seq[Any]], header: Seq[Any], fileName: String, synchronized : Boolean = true, overwrite: Boolean = false): Unit = {
    val fileExists = new File(fileName).exists()
    writeFile(fileName, f => if (fileExists && !overwrite) {
      appendCsv(f, _.writeAll(data))
    } else {
      writeCsv(f, _.writeAll(Seq(header) ++ data))
    }, synchronized)
  }

  import java.io.PrintWriter

  /**
   *
   * @param file input file
   * @param writeFun writing function
   * @param append true if you want to append to existing file, default false
   * @param synchronized true if you want thread-safe write
   */
  private def printFile(file: String, writeFun : PrintWriter => Unit, append : Boolean = false, synchronized: Boolean = false): Unit = {
    writeFile(file, f => {
      val writer = new PrintWriter ( new java.io.FileWriter(f, append))
      writeFun ( writer )
      writer.close ()
    }, synchronized)
  }

  /** Helper for use ${conf.var} in files */
  private object RichBufferedSource {
    implicit class MyList(file: scala.io.BufferedSource) {
      private def substituteConf(string : String, stringMap: Map[String, String]): String =
        "\\$\\{([^}]+)\\}".r.replaceAllIn ( string, m => stringMap ( m.group ( 1 ) ) )

      def lines(stringMap: Map[String, String]): List[String] =
        file.getLines().map(_.replaceAll ( "\\n", "" )).map(x => substituteConf(x, stringMap)).toList
    }
  }
  /**
   *
   * @param source to read file with declarations in form of ${} from configuration
   * @param isResource true if the input file is a resource in project
   * @param save how to save file with configuration solved
   * @param stringMap the map of string to replace in the file
   * @param rowSeparator row separator string, default \n
   */
  def substituteConfigurationAndWriteNewFile (source: String, isResource: Boolean, save: String, stringMap: Map[String, String], rowSeparator : String = "\n"): Unit = {
    import RichBufferedSource._
    printFile ( save, f => FileReader.fromFile ( source,  _.lines (stringMap), isResource ).foreach ( l => f.write(s"$l$rowSeparator") ) )
  }
}
