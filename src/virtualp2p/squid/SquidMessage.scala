package virtualp2p.squid

import rice.p2p.commonapi.Message
import rice.p2p.commonapi.Id

/**
 * User: alejandro
 * Date: 4/04/12
 * Time: 02:52 PM
 */

class SquidMessage(val originId : Id, val destinyId : Id, val data : Array[Byte]) extends Message{
  override def toString : String = {
    "MyMsg from " + originId + " to " + destinyId;
  }

  def getPriority : Int = {
    Message.LOW_PRIORITY;
  }
}
