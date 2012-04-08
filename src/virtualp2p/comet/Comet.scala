package virtualp2p.comet

import scala.util.Marshal
import java.io.{FileNotFoundException, FileInputStream}
import virtualp2p.common._
import virtualp2p.squid.{SquidId, JoinException, SquidNode}
import collection.mutable.LinkedList

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
  // Data structure holding all the tuples
  var tuples : LinkedList[XmlTuple] = new LinkedList[XmlTuple]

  //Load properties
  var input : FileInputStream = null
  try {
    input = new FileInputStream(propertiesFilename);
  } catch {
    case e : FileNotFoundException => {
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
  def join() {
    try {
      squid.join()
      println("Comet node joined succesfully")
      //Register the receive method to listen to the messages from the overlay
      squid.register(receive)
    } catch {
      case e : JoinException => {
        println("Comet: error joining " + e.getMessage)
        sys.exit()
      }
    }
  }

  /**
   * Stores a tuple in the comet space.
   * @param tup The tuple to be stored.
   */
  def in(tup : XmlTuple){
    var id : SquidId =  new SquidId(dimensions, bits, types, tup.getKeys)
    squid.routeTo(id, Marshall.dump(tup))
  }

  /**
   * Reads a tuple from the space, but does not erase it.
   * @param tup The
   */
  def rd(tup : XmlTuple){

  }

  def out(tup : XmlTuple){

  }

  /**
   * Gets a list of tuples matching the given one.
   * @param tup The tuple used as comparisson.
   * @param remove Specify wehter to remove or keep the returned tuples.
   * @return An Array of all tuples matching the given one.
   */
  def get(tup : XmlTuple, remove : Boolean = false) : Array[XmlTuple] = {
    var list : LinkedList[XmlTuple] = new LinkedList[XmlTuple]
    var rest : LinkedList[XmlTuple] = new LinkedList[XmlTuple]
    tuples synchronized{
      tuples.foreach(other => if(tup == other) list :+ other else if(remove) rest :+ other)
      if (remove) tuples = rest
    }
    list
  }

  /**
   * Inserts a new tuple.
   * @param tup The tuple to be inserted
   */
  def insert(tup : XmlTuple) {
    tuples.synchronized{(tup)}
  }

  /**
   * Receives a message from the comet space
   * @param message The message received
   */
  def receive(message: Array[Byte]){
    try{
      val tup: XmlTuple = Marshal.load[XmlTuple](message)
      println("Comet: received message tuple with " + tup.operation + " oepration")
      tup.operation match {
        case OperationTypes.IN => {
          var matched = get(tup, true)
        }
        case OperationTypes.OUT => {
          insert(tup)
        }
        case OperationTypes.RD => {
          var matched = get(tup, false)
        }
      }
    } catch {
      case e : ClassNotFoundException =>
        System.out.println("Comet: invalid message received " + e.getMessage)
    }
  }
}
