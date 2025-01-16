package it.unibo.big.utils

import org.apache.log4j.FileAppender

/**
 * File appender for log4j
 */
class ExecutionFileAppender extends FileAppender {
  override def setFile(file: String): Unit = super.setFile(ExecutionFileAppender.prependDate(file))
}

/**
 * Appender for log4j for add the timestamp on the log file
 */
private object ExecutionFileAppender {
  def prependDate(filename: String): String = s"logs/${System.currentTimeMillis}_$filename"
}