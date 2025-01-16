package it.unibo.big.streamprofiling.algorithm.utils

/**
 * Utility object for set the configuration of the application
 */
object Settings {
  import com.typesafe.config.{Config, ConfigFactory}

  import collection.JavaConverters._

  private var config = ConfigFactory.load()

  /**
   * Modifies the config in application.conf with the key-value pair
   * @param mapConfig the new config map
   */
  def set(mapConfig : Map[String, AnyRef]): Unit = {
    config = ConfigFactory.parseMap(mapConfig.asJava).withFallback(ConfigFactory.parseResources("application.conf")).resolve()
  }

  /**
   *
   * @return actual conf
   */
  def CONFIG: Config = config
}
