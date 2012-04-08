package virtualp2p.common
import scala.xml._

/**
 * User: alejandro
 * Date: 8/04/12
 * Time: 02:06 PM
 */

object OperationTypes extends Enumeration {
  type OperationTypes = Value
  val RD = Value("RD")
  val IN = Value("IN")
  val OUT = Value("OUT")
}


class XmlTuple(hdr : String, data : Array[Byte], operation : String) extends Serializable{
  val header = XML.load(hdr)

  def getKeys : Array[(String, String)] = {
    header \ "keys" \ "key" foreach {(key) =>
    }
  }

  def getSecondary : Array[(String, String)] = {
    header \ "secondary" \ "key" foreach {(key) =>
    }
  }

  /**
   * Decides if the two xml tuples match each other
   */
  def ==(other : XmlTuple) : Boolean = {
    // Compare the keys and secondary keys and see if they match
  }
}
