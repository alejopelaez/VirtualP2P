package virtualp2p.meteor

import collection.mutable.ListBuffer
import java.io.{FileNotFoundException, FileInputStream}
import virtualp2p.squid.{SquidId, JoinException, SquidNode}
import util.Marshal
import rice.p2p.commonapi.NodeHandle
import virtualp2p.common.{PropertiesLoader, OperationTypes, Logger, XmlTuple}
import java.util.{Properties, Date}

/**
 * User: alejandro
 * Date: 14/05/12
 * Time: 02:22 PM
 */

/**
 * Class that represents a node on the meteor overlay
 * @param properties The properties of this node
 */
class Meteor(properties : Properties, defaultPort : String = "9000") {
  //Other properties
  def this(filename : String) = this(PropertiesLoader.load(filename))
  def this() = this("config/squid.properties")

  var messagesSent = 0
  var messagesReceived = 0

  var callback : (Array[Byte], Date) => Unit = null

  // Data structure holding all the tuples
  var tuples : ListBuffer[XmlTuple] = new ListBuffer[XmlTuple]

  val joined = false;
  val squid : SquidNode = new SquidNode(properties, defaultPort)

  val types: Array[String] = properties.getProperty("keyTypes","numeric").split(",")
  val bits: Int =  Integer.parseInt(properties.getProperty("bitLength","160"))
  val dimensions: Int =  Integer.parseInt(properties.getProperty("dimensions","1"))

  /**
   * Joins this comet node to the overlay
   */
  def join() {
    try {
      squid.join()
      Logger.println("node joined succesfully", "Meteor")
      //Register the receive method to listen to the messages from the overlay
      squid.register(receive)
    } catch {
      case e : JoinException => {
        println("Meteor: error joining " + e.getMessage)
        sys.exit()
      }
    }
  }

  /**
   * Reads a tuple from the space, but does not erase it.
   * @param tup The tuple to match against.
   */
  def subscribe(tup : XmlTuple, date : Date) {
    tup.operation = OperationTypes.SUBSCRIBE.toString
    tup.from = squid.endPoint.getLocalNodeHandle
    tup.date = date
    val id : SquidId =  new SquidId(dimensions, bits, types, tup.getKeys)
    messagesSent += 1
    squid.routeTo(id, Marshal.dump(Array(tup)))
  }

  /**
   * Reads a tuple from the space, but does not erase it.
   * @param tup The tuple to match against.
   */
  def unsubscribe(tup : XmlTuple, date : Date) {
    tup.operation = OperationTypes.UNSUBSCRIBE.toString
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
  def publish(tup : XmlTuple, date : Date){
    tup.operation = OperationTypes.PUBLISH.toString
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
          if(other == tup)
            list += other
      })
    }
    list.toArray
  }

  def remove(tup : XmlTuple){
    var list : ListBuffer[XmlTuple] = new ListBuffer[XmlTuple]
    tuples synchronized{
      tuples.foreach(other => {
        if(!(tup == other && tup.from.getId.equals(other.from.getId)))
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
    tuples.synchronized{
      if (insert){
        Logger.println("Subscribing " + tup.from, "Meteor")
        tuples += tup
      } else {
        /*Logger.println("Tuple with id " + tup.getId + " ealready existed, updating it instead", "Meteor")
        orig.data = tup.data
        orig.header = tup.header*/
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
        Logger.println("Received message tuple with " + tup.operation + " oepration", "Meteor")
        OperationTypes.withName(tup.operation) match {
          case OperationTypes.PUBLISH => {
            val matches = get(tup)
            matches.foreach(t => respond(Array(tup), t.from, tup.date))
          }
          case OperationTypes.SUBSCRIBE => {
            insert(tup)
          }
          case OperationTypes.UNSUBSCRIBE => {
            remove(tup)
          }
          case OperationTypes.RES => {
            notify(tup.data, tup.date)
          }
          case _ => Logger.println("Received tuple with an unknown operation " + tup.operation, "Meteor")
        }
      })
    } catch {
      case e : ClassNotFoundException =>
        println("Meteor: invalid message received " + e.getMessage)
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
      Logger.println("Meteor: Warning - Callback is nil, did you forget to register the comet object?")
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
