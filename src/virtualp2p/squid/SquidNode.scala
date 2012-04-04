package virtualp2p.squid

import java.util.Properties
import rice.pastry.standard.RandomNodeIdFactory
import rice.pastry.socket.SocketPastryNodeFactory
import rice.environment.Environment
import java.net.{InetAddress, InetSocketAddress}
import scala.Predef._
import java.io.{IOException, FileNotFoundException, FileInputStream}
import rice.p2p.commonapi.{Id, Endpoint, NodeHandle}
import rice.pastry.{Id, NodeIdFactory, PastryNode}
import rice.pastry.commonapi.PastryIdFactory
import virtualp2p.tutorial.MyMsg

/**
 * User: alejandro
 * Date: 3/04/12
 * Time: 06:20 PM
 */

/**
 * Class that represents a node on the squid overlay
 * @param propertiesFilename Path to the file with the properties of this node
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

  val BIT_LENGTH   = Integer.parseInt(properties.getProperty("bitLength", "16"))

  val env = new Environment()

  var node : PastryNode = null
  var endPoint : Endpoint = null

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
  def join {
    // construct the PastryNodeFactory, this is how we use rice.pastry.socket
    val factory = new SocketPastryNodeFactory(new RandomNodeIdFactory(env), localPort, env)

    // construct a node, but this does not cause it to boot
    node = factory.newNode();
    endpoint = node.buildEndpoint(this, "myinstance");

    // Determines if joining a ring or create a new one
    if (properties.getProperty("bootstrap", "false") == "true"){
      println("Starting as bootstrap node...")
      val bootHandle : NodeHandle = null
      node.boot(bootHandle)
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
          sys.exit
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
   * @param id The squid id of the destination
   */
  def routeTo(id : SquidId) {
    if (!id.hasRanges) {
      routeSimple(id)
    } else {
      routeComplex(id)
    }
  }

  /**
   * Routes a message through the squid overlay where the key is assumed
   * to be free of ranges.
   * @param id The squid id of the destination
   */
  def routeSimple(id : SquidId) {
    val chordIndex : BigInt = HilbertSFC.coordinatesToIndex(id.getKeyBits);
    val factory : PastryIdFactory = new PastryIdFactory
    val id : Id = factory.buildId(chordIndex.toByteArray)
    System.out.println(this + " sending direct to " + id);
    val msg : SquidMessage = new SquidMessage(endpoint.getId, id);
    endpoint.route(null, msg, id);
    chord.routeTo(chord.generateID(chordIndex), "squid", SquidMessage.newUserMessage(peers, localNode, tag, payload).serialize());
  }
}
