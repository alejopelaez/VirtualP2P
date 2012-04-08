package virtualp2p.comet

import java.io.{FileNotFoundException, FileInputStream}
import virtualp2p.common._
import virtualp2p.squid.{SquidId, JoinException, SquidNode}

/**
 * User: alejandro
 * Date: 8/04/12
 * Time: 01:44 PM
 */

/**
 * Class that represents a node on the comet overlay
 * @param propertiesFilename Path to the file with the properties of this node
 */
class Comet(propertiesFilename : String) {
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
  System.getProperties.load(input)
  val properties = System.getProperties

  val joined = false;
  val squid : SquidNode = new SquidNode(propertiesFilename)

  val types: Array[String] = properties.getProperty("keyTypes","numeric").split(",")
  val bits: Int =  Integer.parseInt(properties.getProperty("bitLength","160"))
  val dimensions: Int =  Integer.parseInt(properties.getProperty("dimensions","1"))

  /**
   * Constructor that looks for the properties file in the default location.
   */
  def this() = this("config/squid.properties")

  /**
   * Joins this comet node to the overlay
   */
  def join {
    try {
      squid.join
      println("Comet node joined succesfully")
      squid.register(receive)
    } catch {
      case e : JoinException => {
        println("Comet: error joining " + e.getMessage)
        sys.exit
      }
    }
  }

  def in(tup : XmlTuple){
    var id : SquidId =  new SquidId(dimensions, bits, types, Array(("12", "12"), ("df", "df")))
    squid.routeTo(id, "hola".getBytes)
  }

  def rd(tup : XmlTuple){

  }

  def out(tup : XmlTuple){

  }

  def receive(message: Array[Byte]){
    println("Comet: received message here :)" + String(message))
  }
}
