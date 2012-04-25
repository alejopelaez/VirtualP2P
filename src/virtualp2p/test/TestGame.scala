package virtualp2p.test

import virtualp2p.game.{Engine}
import virtualp2p.common.XmlTuple
import util.Marshal

/**
 * User: alejandro
 * Date: 8/04/12
 * Time: 02:26 PM
 */

object TestGame {

  def main(args: Array[String]){
    var engine : Engine = new Engine

    engine.start
  }
}
