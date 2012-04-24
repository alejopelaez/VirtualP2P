package virtualp2p.common
import scala.xml._
import collection.mutable.ListBuffer

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


class XmlTuple(var header : Elem, var data : Array[Byte], var operation: String = "IN") extends Serializable{

  var from : String = ""

  def getKeys : Array[(String, String)] = {
    var keys : ListBuffer[(String, String)] = new ListBuffer[(String, String)]
    (header \\ "keys" \ "_") foreach {(key) =>
      keys.append((key.text, key.label))
    }
    keys.toArray
  }

  def getSecondary : Array[(String, String)] = {
    var keys : ListBuffer[(String, String)] = new ListBuffer[(String, String)]
    (header \\ "secondary" \ "_") foreach {(key) =>
      keys.append((key.text, key.label))
    }
    keys.toArray
  }

  /**
   * Decides if the two xml tuples match each other
   */
  def ==(other : XmlTuple) : Boolean = {
    val myKeys = getKeys ++ getSecondary
    val otKeys = other.getKeys ++ other.getSecondary
    for (i : Int <- 0 to myKeys.size - 1){
      val myKey = myKeys(i)
      val otKey = otKeys(i)
      if (myKey._1 != otKey._1)
        return false
      else if (myKey._2 != "*" && myKey._2 != otKey._2)
        return false
    }
    true
  }
}
