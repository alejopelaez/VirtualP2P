package virtualp2p.comet

import scala.util.Marshal
import java.io.{FileNotFoundException, FileInputStream}
import virtualp2p.common._
import virtualp2p.squid.{SquidId, JoinException, SquidNode}
import scala.Array
import collection.mutable.ListBuffer
import rice.p2p.commonapi.NodeHandle
import java.util.Date

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
  var messagesSent = 0
  var messagesReceived = 0

  var callback : (Array[Byte], Date) => Unit = null

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
      Logger.println("node joined succesfully", "Comet")
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
   * Reads a tuple from the space and erases it.
   * @param tup The tuple to match against.
   * @param blocking Run this as a blocking or non-blocking operation.
   */
  def in(tup : XmlTuple, date : Date, blocking : Boolean = false){
    tup.from = squid.endPoint.getLocalNodeHandle
    tup.date = date
    val id : SquidId =  new SquidId(dimensions, bits, types, tup.getKeys)
    messagesSent += 1
    squid.routeTo(id, Marshal.dump(Array(tup)))
  }

  /**
   * Reads a tuple from the space, but does not erase it.
   * @param tup The tuple to match against.
   * @param blocking Run this as a blocking or non-blocking operation.
   */
  def rd(tup : XmlTuple, date : Date, blocking : Boolean = false) {
    tup.operation = OperationTypes.RD.toString
    tup.from = squid.endPoint.getLocalNodeHandle
    tup.date = date
    val id : SquidId =  new SquidId(dimensions, bits, types, tup.getKeys)
    messagesSent += 1
    squid.routeTo(id, Marshal.dump(Array(tup)))
  }

  /**
   * Inserts a tuple into the space.
   * @param tup The tuple to match against.
   */
  def out(tup : XmlTuple, date : Date){
    tup.operation = OperationTypes.OUT.toString
    tup.from = squid.endPoint.getLocalNodeHandle
    tup.date = date
    val id : SquidId =  new SquidId(dimensions, bits, types, tup.getKeys)
    messagesSent += 1
    squid.routeTo(id, Marshal.dump(Array(tup)))
  }

  /**
   * Gets a list of tuples matching the given one.
   * @param tup The tuple used as comparisson.
   * @return An Array of all tuples matching the given one.
   */
  def get(tup : XmlTuple) : Array[XmlTuple] = {
    var list : ListBuffer[XmlTuple] = new ListBuffer[XmlTuple]
    tuples synchronized{
      tuples.foreach(other => {
        if (tup.from.getId.toString != other.from.getId.toString)
          if(tup == other)
            list += other
      })
    }
    list.toArray
  }

  def remove(tup : XmlTuple){
    var list : ListBuffer[XmlTuple] = new ListBuffer[XmlTuple]
    tuples synchronized{
      tuples.foreach(other => {
          if(!(tup == other))
            list += other
      })
    }
    tuples = list
  }

  /**
   * Inserts a new tuple.
   * @param tup The tuple to be inserted.
   */
  def insert(tup : XmlTuple) {
    var insert = true
    var orig : XmlTuple = null
    tuples.foreach(tuple => {
      if (tup.getId == tuple.getId){
        insert = false
        orig = tuple
      }
    })
    tuples.synchronized{
      if (insert){
        Logger.println("Inserting tuple with id " + tup.getId, "Comet")
        tuples += tup
      } else {
        Logger.println("Tuple with id " + tup.getId + " ealready existed, updating it instead", "Comet")
        orig.data = tup.data
        orig.header = tup.header
      }
    }
  }

  def respond(tups : Array[XmlTuple], node : NodeHandle, date : Date){
    if (tups.size > 0) {
      tups.foreach(tup => {
        tup.operation = OperationTypes.RES.toString
        tup.date = date
      })
      messagesSent += 1
      squid.direct(node, Marshal.dump(tups))
    }
  }

  /**
   * Receives a message from the comet space
   * @param message The message received.
   */
  def receive(message: Array[Byte]){
    messagesReceived += 1
    try{
      val tups: Array[XmlTuple] = Marshal.load[Array[XmlTuple]](message)
      tups.foreach(tup => {
        Logger.println("Received message tuple with " + tup.operation + " oepration", "Comet")
        OperationTypes.withName(tup.operation) match {
          case OperationTypes.IN => {
            remove(tup)
          }
          case OperationTypes.OUT => {
            insert(tup)
          }
          case OperationTypes.RD => {
            var matched = get(tup)
            if (matched.size > 0) respond(matched, tup.from, tup.date)
          }
          case OperationTypes.RES => {
            notify(tup.data, tup.date)
          }
          case _ => Logger.println("Received tuple with an unknown operation " + tup.operation, "Comet")
        }
      })
    } catch {
      case e : ClassNotFoundException =>
        println("Comet: invalid message received " + e.getMessage)
    }
  }

  /**
   * Calls the callback function with the data.
   * @param data The data to send to the callback function.
   */
  def notify(data : Array[Byte], date : Date){
    if (callback != null){
      callback(data, date)
    } else {
      Logger.println("Comet: Warning - Callback is nil, did you forget to register the comet object?")
    }
  }

  /**
   * Register to comet to be able to receive data from the non blocking functions.
   * @param func The callback to notify.
   */
  def register(func : (Array[Byte], Date) => Unit){
    callback = func
  }

  def numberStored : Int = {
    tuples.size
  }
}
