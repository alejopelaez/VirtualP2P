package virtualp2p.common

import collection.mutable.HashMap
import java.io.{FileNotFoundException, FileInputStream}
import java.util.Properties

/**
 * User: alejandro
 * Date: 1/05/12
 * Time: 05:10 PM
 */

/**
 * Simple object that handles logging logic of several spaces at a time
 */
object Logger {
  var enabled = new HashMap[String, Boolean].withDefaultValue(false)

  def setProperties(filename : String = "config/logger.properties"){
    //Load properties
    var input : FileInputStream = null
    try {
      input = new FileInputStream(filename);
    } catch {
      case
        e : FileNotFoundException => {
        println("Logger constructor: " + e.getMessage)
        sys.exit()
      }
    }
    System.getProperties.load(input)
    val properties : Properties = System.getProperties
    properties.getProperty("spaces").split(",").foreach(space => enabled(space) = true)
  }

  def enable(space : String) {
    enabled(space) = true
  }
  def disable(space : String) {
    enabled(space) = false
  }

  def println(message : String, space : String) {
    if (enabled(space)){
      print(space + ": " + message + "\n")
    }
  }
  def println(message : String) {
    println(message)
  }
}
