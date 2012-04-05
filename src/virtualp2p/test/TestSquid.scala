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
    squidNode.routeTo(new SquidId(1, 32, Array("numeric"), Array(("93361", "93361"))), "asdf".getBytes)
    squidNode.routeTo(new SquidId(1, 32, Array("alphabetic"), Array(("hola", "hola"))), "asdf".getBytes)
    squidNode.routeTo(new SquidId(1, 32, Array("numeric"), Array(("99993122", "99993122"))), "holaaa".getBytes)

    var id : SquidId =  new SquidId(1, 32, Array("numeric"), Array(("93361", "93369")))
    id.setKey(0, ("93361", "93369"))
    squidNode.routeTo(id, "complejo".getBytes())
    //sys.exit
  }
}
