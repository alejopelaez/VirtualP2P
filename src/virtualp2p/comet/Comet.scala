package virtualp2p.comet

import scala.util.Marshal
import java.io.{FileNotFoundException, FileInputStream}
import virtualp2p.common._
import virtualp2p.squid.{SquidId, JoinException, SquidNode}
import scala.Array
import collection.mutable.ListBuffer
import rice.p2p.commonapi.NodeHandle

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
  var callback : Array[Byte] => Unit = null

  // Data structure holding all the tuples
  var tuples : ListBuffer[XmlTuple] = new ListBuffer[XmlTuple]

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
    tup.from = squid.endPoint.getLocalNodeHandle
    var id : SquidId =  new SquidId(dimensions, bits, types, tup.getKeys)
    squid.routeTo(id, Marshal.dump(Array(tup)))
  }

  /**
   * Reads a tuple from the space, but does not erase it.
   * @param tup The tuple to match against.
   * @param blocking Run this as a blocking or non-blocking operation.
   */
  def rd(tup : XmlTuple, blocking : Boolean = false) {
    tup.operation = OperationTypes.RD.toString
    tup.from = squid.endPoint.getLocalNodeHandle
    var id : SquidId =  new SquidId(dimensions, bits, types, tup.getKeys)
    squid.routeTo(id, Marshal.dump(Array(tup)))
  }

  /**
   * Reads a tuple from the space and erases it.
   * @param tup The tuple to match against.
   * @param blocking Run this as a blocking or non-blocking operation.
   */
  def out(tup : XmlTuple, blocking : Boolean = false){
    tup.operation = OperationTypes.OUT.toString
    tup.from = squid.endPoint.getLocalNodeHandle
    var id : SquidId =  new SquidId(dimensions, bits, types, tup.getKeys)
    squid.routeTo(id, Marshal.dump(Array(tup)))
  }

  /**
   * Gets a list of tuples matching the given one.
   * @param tup The tuple used as comparisson.
   * @param remove Specify wehter to remove or keep the returned tuples.
   * @return An Array of all tuples matching the given one.
   */
  def get(tup : XmlTuple, remove : Boolean = false) : Array[XmlTuple] = {
    var list : ListBuffer[XmlTuple] = new ListBuffer[XmlTuple]
    var rest : ListBuffer[XmlTuple] = new ListBuffer[XmlTuple]
    tuples synchronized{
      tuples.foreach(other => if(tup == other) list += other else if(remove) rest += other)
    }
    if (remove) tuples = rest
    list.toArray
  }

  /**
   * Inserts a new tuple.
   * @param tup The tuple to be inserted.
   */
  def insert(tup : XmlTuple) {
    tuples.synchronized{
      tuples += tup
    }
  }

  def respond(tups : Array[XmlTuple], node : NodeHandle){
    if (tups.size > 0) {
      tups.foreach(tup => tup.operation = OperationTypes.RES.toString)
      squid.direct(node, Marshal.dump(tups))
    }
  }

  /**
   * Receives a message from the comet space
   * @param message The message received.
   */
  def receive(message: Array[Byte]){
    try{
      val tups: Array[XmlTuple] = Marshal.load[Array[XmlTuple]](message)
      tups.foreach(tup => {
        println("Comet: received message tuple with " + tup.operation + " oepration")
        OperationTypes.withName(tup.operation) match {
          case OperationTypes.IN => {
            var matched = get(tup, true)
            respond(matched, tup.from)
          }
          case OperationTypes.OUT => {
            insert(tup)
          }
          case OperationTypes.RD => {
            var matched = get(tup, false)
            respond(matched, tup.from)
          }
          case OperationTypes.RES => {
            notify(tup.data)
          }
          case _ => println("Comet: Received tuple with an unknown operation " + tup.operation)
        }
      })
    } catch {
      case e : ClassNotFoundException =>
        System.out.println("Comet: invalid message received " + e.getMessage)
    }
  }

  /**
   * Calls the callback function with the data.
   * @param data The data to send to the callback function.
   */
  def notify(data : Array[Byte]){
    if (callback != null){
      callback(data)
    } else {
      println("Comet: Warning - Callback is nil, did you forget to register the comet object?")
    }
  }

  /**
   * Register to comet to be able to receive data from the non blocking functions.
   * @param func The callback to notify.
   */
  def register(func : Array[Byte] => Unit){
    callback = func
  }
}
