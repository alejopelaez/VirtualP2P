package virtualp2p.tutorial


import rice.p2p.commonapi.Id
import rice.p2p.commonapi.Message
/**
 * User: alejandro
 * Date: 3/04/12
 * Time: 01:10 PM
 */

class MyMsg(from : Id, to : Id) extends Message{

  override def toString : String = {
    "MyMsg from " + from + " to " + to;
  }

  def getPriority : Int = {
    Message.LOW_PRIORITY;
  }
}
