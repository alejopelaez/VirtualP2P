package virtualp2p.squid

import java.util.Properties
import java.io.{FileNotFoundException, FileInputStream}

/**
 * User: alejandro
 * Date: 3/04/12
 * Time: 06:20 PM
 */

class SquidNode(propertiesFilename : String) {
  //Load properties
  var input : FileInputStream = null
  try {
    input = new FileInputStream(propertiesFilename);
  } catch {
    case
      e : FileNotFoundException => {
        println("SquidNode constructor: " + e.getMessage)
        sys.exit()
    }
  }
  var properties : Properties = new Properties
  properties.load(input)


  def this() = this("config/squid.properties")
}
