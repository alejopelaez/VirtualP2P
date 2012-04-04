package virtualp2p.test

import virtualp2p.squid._

/**
 * User: alejandro
 * Date: 3/04/12
 * Time: 06:54 PM
 */

object TestSquid{
  def main(args: Array[String]){
    var squidNode : SquidNode = new SquidNode()
    squidNode.join
    sys.exit
  }
}
