package test

import java.net._;
import java.io.IOException;
import rice.pastry._;
import rice.environment.Environment;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.pastry.standard.RandomNodeIdFactory;

/**
 * User: alejandro
 * Date: 27/03/12
 * Time: 06:21 PM
 */

class PastryTest(bindPort: Int, bootAddress: InetSocketAddress, env: Environment) {
  // Generate the NodeIds Randomly
  val nidFactory = new RandomNodeIdFactory(env);

  // construct the PastryNodeFactory, this is how we use rice.pastry.socket
  val factory = new SocketPastryNodeFactory(nidFactory, bindPort, env);

  // construct a node, but this does not cause it to boot
  val node = factory.newNode();

  // in later tutorials, we will register applications before calling boot
  node.boot(bootAddress);

  // the node may require sending several messages to fully boot into the ring
  node.synchronized {
    while(!node.isReady && !node.joinFailed) {
      // delay so we don't busy-wait
      node.wait(500);

      // abort if can't join
      if (node.joinFailed) {
        throw new IOException {
          "Could not join the FreePastry ring.  Reason:" + node.joinFailedReason
        };
      }
    }
  }

   println("Finished creating new node "+node);
}

object Test{
  def main(args: Array[String]){
    // Loads pastry settings
    val env = new Environment();

    // disable the UPnP setting (in case you are testing this on a NATted LAN)
    env.getParameters.setString("nat_search_policy","never");

    try {
      // the port to use locally
      val bindPort = Integer.parseInt(args(0));

    // build the bootAddress from the command line args
    val bootAddr = InetAddress.getByName(args(1));
    val bootPort = Integer.parseInt(args(2));
    val bootAddress = new InetSocketAddress(bootAddr,bootPort);

    // launch our node!
    val dt = new PastryTest(bindPort, bootAddress, env);
    } catch {
      case e: Exception => {
        // remind user how to use
        println("Usage:");
        println("scala [-cp FreePastry-.jar] rice.tutorial.lesson1.DistTutorial localbindport bootIP bootPort");
        println("Example scala rice.tutorial.DistTutorial 9001 pokey.cs.almamater.edu 9001");
        throw e;
      }
    }
  }
}
