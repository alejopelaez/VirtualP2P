package virtualp2p.test

import virtualp2p.game.{Engine}
import util.Marshal
import virtualp2p.common.{Logger, XmlTuple}

/**
 * User: alejandro
 * Date: 8/04/12
 * Time: 02:26 PM
 */

object TestGame {

  def main(args: Array[String]){
    var engine : Engine = new Engine
    Logger.setProperties()
    engine.start
  }
}
