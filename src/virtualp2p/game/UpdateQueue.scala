package virtualp2p.game

import com.jme3.math.Transform
import collection.mutable.ListBuffer

/**
 * User: alejandro
 * Date: 1/05/12
 * Time: 03:56 PM
 */

/**
 * Class that hold the changes that needs to be done to the state of the simulation.
 */
object UpdateQueue {
  var messages : ListBuffer[UpdateMessage] = new ListBuffer[UpdateMessage]

  /**
   * Queue an update for later processing
   * @param message The pending update
   */
  def queueUpdate(message : UpdateMessage) {
    message.synchronized{
      messages += message
    }
  }

  /**
   * Returns the pending updates and clears the job queue
   * @return List of pending updates
   */
  def pendingUpdates : Array[UpdateMessage] = {
    val array = messages.toArray
    messages.clear()
    array
  }
}
