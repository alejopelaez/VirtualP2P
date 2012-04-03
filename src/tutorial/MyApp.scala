package tutorial

import rice.p2p.commonapi.Application;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.Node;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.RouteMessage;

/**
 * User: alejandro
 * Date: 3/04/12
 * Time: 01:45 PM
 */

class MyApp(node : Node) extends Application{
  /**
  * The Endpoint represents the underlying node.  By making calls on the
  * Endpoint, it assures that the message will be delivered to a MyApp on whichever
  * node the message is intended for.
  */
  var endpoint : Endpoint = node.buildEndpoint(this, "myinstance");

  // now we can receive messages
  endpoint.register();

  /**
   * Called to route a message to the id
   */
  def routeMyMsg(id : Id) = {
    System.out.println(this+" sending to "+id);
    val msg : MyMsg = new MyMsg(endpoint.getId, id);
    endpoint.route(id, msg, null);
  }

  /**
   * Called to directly send a message to the nh
   */
  def routeMyMsgDirect(nh : NodeHandle) = {
    System.out.println(this + " sending direct to " + nh);
    val msg : MyMsg = new MyMsg(endpoint.getId, nh.getId);
    endpoint.route(null, msg, nh);
  }

  /**
   * Called when we receive a message.
   */
  def deliver(id : Id, message : Message) {
    System.out.println(this + " received " + message);
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

  override def toString : String = {
    "MyApp " + endpoint.getId;
  }
}
