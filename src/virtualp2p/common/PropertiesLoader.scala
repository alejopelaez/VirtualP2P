package virtualp2p.common

import java.util.Properties
import java.io.{FileNotFoundException, FileInputStream}

/**
 * User: alejandro
 * Date: 17/05/12
 * Time: 06:13 PM
 */

object PropertiesLoader{
  def load(filename : String) : Properties = {
    //Load properties
    var input : FileInputStream = null
    try {
      input = new FileInputStream(filename);
    } catch {
      case
        e : FileNotFoundException => {
        println("SquidNode constructor: " + e.getMessage)
        sys.exit()
      }
    }
    val properties = new Properties()
    properties.load(input)
    input.close
    properties
  }
}
