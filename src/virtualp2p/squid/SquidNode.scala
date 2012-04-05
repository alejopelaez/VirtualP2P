package virtualp2p.squid

import scala.Predef._

import java.util.{Properties}
import java.net.{InetAddress, InetSocketAddress}
import java.io.{IOException, FileNotFoundException, FileInputStream}

import rice.pastry.standard.RandomNodeIdFactory
import rice.pastry.socket.SocketPastryNodeFactory
import rice.environment.Environment
import rice.pastry.{NodeIdFactory, PastryNode}
import rice.pastry.commonapi.PastryIdFactory
import rice.p2p.commonapi.{Endpoint, NodeHandle, Message, RouteMessage, Application, Id}
import java.math.BigInteger

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
  val properties = System.getProperties

  val BIT_LENGTH   = Integer.parseInt(properties.getProperty("bitLength", "16"))

  val env = new Environment("config/pastry")

  var node : PastryNode = null
  var endPoint : Endpoint = null

  var mapping : HilbertSFC = new HilbertSFC

  var joined : Boolean = false

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
      println("Starting as bootstrap node...")
      val bootHandle : NodeHandle = null
      node.boot {bootHandle}
    }else{
      val bootAddr = InetAddress.getByName(properties.getProperty("bootAddress"))
      val bootPort = Integer.parseInt(properties.getProperty("bootPort", "9000"))
      val bootAddress = new InetSocketAddress(bootAddr,bootPort);
      println("Connecting node to an existing pastry ring at " + bootAddress)
      node.boot(bootAddress);
    }

    // the node may require sending several messages to fully boot into the ring
    node.synchronized {
      while(!node.isReady && !node.joinFailed) {
        // delay so we don't busy-wait
        node.wait(500);

        // abort if can't join
        if (node.joinFailed) {
          println("Could not join the FreePastry ring.  Reason:" + node.joinFailedReason)
          sys.exit()
        }
      }
    }
    joined = true

    if (properties.getProperty("bootstrap", "false") == "true")
      println("Succesfully created the ring")
    else
      println("Succesfully joined the ring")
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
    val id : Id = factory.buildId(mapping.indexToArray(index))

    System.out.println(endPoint.getId + " sending to " + id);
    val msg : SquidMessage = new SquidMessage(endPoint.getId, id, data);
    endPoint.route(id, msg, null);
  }

  /**
    * Routes a message through the squid overlay where the key is assumed
    * to have ranges.
   * @param squidId The squid id of the destination(s).
   * @param data The data to be sent.
   */
  def routeComplex(squidId : SquidId, data : Array[Byte]) {
    val range : Array[(BigInt, BigInt)] = squidId.getKeyRanges();
    var refinement = 0;
    var partialIndex : BigInt = null;
    var refineFurther : Boolean = true;
    var doDeliver : Boolean = false;

    if (refinement > 0) {
      if (covers(partialIndex, refinement)) {
        refineFurther = false;
        doDeliver = true;
      }
      else {
        val coords : Array[BigInt] = mapping.indexToCoordinates(new BigInteger(endPoint.getId.toString));
        boolean included = true;
        for (i <- 0 to coords.length - 1)
          if (coords(i) < range(i)._1 || coords(i) > range(i)._2)
            included = false;

        if (included)
          doDeliver = true;
      }

      if (refineFurther) {
        val refiner : ClusterRefiner = new ClusterRefiner(mapping);
        refiner.refine(range, refinement+1);
        val numDestinations : Int = refiner.getDivisionSize();
        for (i <- 0 to numDestinations - 1) {
          val msg : Array[Byte] = SquidMessage.newRoutingMessage(refiner.getRange(i), refiner.currentRefinement(), refiner.getIndex(i), userMessage).serialize();
          endPoint.route(refiner.getIndex(i), msg, null);
          //chord.routeTo(chord.generateID(refiner.getIndex(i)), "squid", chordMessage);
        }
      }
      if (doDeliver) {
        deliverMessage(userMessage);
      }
    }
  }

  /**
   * Called when we receive a message.
   */
  def deliver(id : Id, message : Message) {
    val squidMessage : SquidMessage = message.asInstanceOf[SquidMessage]
    System.out.println(this + " received " + new String(squidMessage.data));
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
}
