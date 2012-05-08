package virtualp2p.squid

import virtualp2p.common.Logger

import java.io.{FileNotFoundException, FileInputStream}
import scala.Predef._

import java.util.Properties
import java.net.{InetAddress, InetSocketAddress}

import rice.pastry.standard.RandomNodeIdFactory
import rice.pastry.socket.SocketPastryNodeFactory
import rice.environment.Environment
import rice.pastry.PastryNode
import rice.pastry.commonapi.PastryIdFactory
import rice.p2p.commonapi.{Endpoint, NodeHandle, Message, RouteMessage, Application, Id}
import java.math.BigInteger
import java.security.MessageDigest

/**
 * User: alejandro
 * Date: 3/04/12
 * Time: 06:20 PM
 */

/**
 * Class that represents a node on the squid overlay
 * @param propertiesFilename Path to the file with the properties of this node
 */
class SquidNode(propertiesFilename : String) extends Application{
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
  val properties : Properties = System.getProperties

  val env = new Environment("config/pastry")

  var node : PastryNode = null
  var endPoint : Endpoint = null

  var mapping : HilbertSFC = new HilbertSFC

  var joined : Boolean = false

  var callback : Array[Byte] => Unit = null

  // The port
  val localPort : Int = Integer.parseInt(properties.getProperty("localPort", "9000"))

  /**
   * Constructor that looks for the properties file in the default location.
   */
  def this() = this("config/squid.properties")

  /**
   * Joins a ring.
   * If bootstrap property is set to true creates a new ring, else it tries to join an existing ring
   * using the bootAddress and bootPort properties.
   */
  def join() {
    // construct the PastryNodeFactory, this is how we use rice.pastry.socket
    val factory = new SocketPastryNodeFactory(new RandomNodeIdFactory(env), localPort, env)

    // construct a node, but this does not cause it to boot
    node = factory.newNode();
    endPoint = node.buildEndpoint(this, "myinstance");
    endPoint.register();

    // Determines if joining a ring or create a new one
    if (properties.getProperty("bootstrap", "false") == "true"){
      Logger.println("Starting as bootstrap node...", "SquidNode")
      val bootHandle : NodeHandle = null
      node.boot {bootHandle}
    }else{
      val bootAddr = InetAddress.getByName(properties.getProperty("bootAddress"))
      val bootPort = Integer.parseInt(properties.getProperty("bootPort", "9000"))
      val bootAddress = new InetSocketAddress(bootAddr,bootPort);
      Logger.println("Connecting node to an existing pastry ring at " + bootAddress, "SquidNode")
      node.boot(bootAddress);
    }

    // the node may require sending several messages to fully boot into the ring
    node.synchronized {
      while(!node.isReady && !node.joinFailed) {
        // delay so we don't busy-wait
        node.wait(500);

        // abort if can't join
        if (node.joinFailed) {
          Logger.println("Could not join the FreePastry ring.  Reason:" + node.joinFailedReason)
          throw new JoinException("Could not join the FreePastry ring.  Reason:" + node.joinFailedReason)
        }
      }
    }
    joined = true

    if (properties.getProperty("bootstrap", "false") == "true")
      Logger.println("Succesfully created the ring at " + node.getLocalNodeHandle, "SquidNode")
    else
      Logger.println("Succesfully joined the ring", "SquidNode")
  }

  /**
   * Sends a direct message to a node.
   * @param node The node handle of the node.
   * @param data The data to be sent.
   */
  def direct(node : NodeHandle, data : Array[Byte]){
    Logger.println(endPoint.getId + " sending direct to " + node.getId, "SquidNode");
    val msg : SquidMessage = new SquidMessage(endPoint.getId, node.getId, data);
    endPoint.route(null, msg, node);
  }

  /**
   * Routes a message through the squid overlay
   * @param squidId The squid id of the destination
   */
  def routeTo(squidId : SquidId, data : Array[Byte]) {
    if (!squidId.hasRanges) {
      routeSimple(squidId, data)
    } else {
      routeComplex(squidId, data)
    }
  }
  def routeTo(squidId : SquidId, msg : SquidMessage) {
    routeTo(squidId, msg.data)
  }

  /**
   * Routes a message through the squid overlay where the key is assumed
   * to be free of ranges.
   * @param squidId The squid id of the destination
   * @param data The data to be sent.
   */
  def routeSimple(squidId : SquidId, data : Array[Byte]) {

    val index: BigInteger = mapping.coordinatesToIndex {
      squidId.getKeyBits.map(x => new BigInteger(x.toString()))
    }
    val factory : PastryIdFactory = new PastryIdFactory(env)

    val md = MessageDigest.getInstance("SHA1")
    md.update(index.toByteArray)
    val id : Id = factory.buildId(md.digest)
    //val id : Id = factory.buildId(mapping.indexToArray(index))

    Logger.println(endPoint.getId + " sending to " + id, "SquidNode");
    val msg : SquidMessage = new SquidMessage(endPoint.getId, id, data);


    endPoint.route(id, msg, null);
  }
  def routeSimple(squidId : SquidId, msg : SquidMessage) {
    routeSimple(squidId, msg.data)
  }

  /**
    * Routes a message through the squid overlay where the key is assumed
    * to have ranges.
   * @param squidId The squid id of the destination(s).
   * @param data The data to be sent.
   */
  def routeComplex(squidId : SquidId, data : Array[Byte]) {
  }
  def routeComplex(squidId : SquidId, msg : SquidMessage) {
  }

  /**
   * Called when we receive a message.
   */
  def deliver(id : Id, message : Message) {
    val squidMessage : SquidMessage = message.asInstanceOf[SquidMessage]
    Logger.println(this.endPoint.getId + " received message " + squidMessage, "SquidNode");
    callback(squidMessage.data)
  }

  /**
   * Called when you hear about a new neighbor.
   * Don't worry about this method for now.
   */
  def update(handle : NodeHandle, joined : Boolean) {
  }

  /**
   * Called a message travels along your path.
   * Don't worry about this method for now.
   */
  def forward(message : RouteMessage) : Boolean = {
    true;
  }

  def register(func : Array[Byte] => Unit) {
    callback = func
  }
}

/**
 * Class to hold the joining exception
 */
class JoinException(msg:String) extends Exception(msg)
