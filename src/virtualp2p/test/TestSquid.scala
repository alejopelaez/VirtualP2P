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

    // To send a mesasge through squid, the types array and data arrays has to be constructed.
    var id : SquidId =  new SquidId(2, 160, Array("alphabetic", "alphabetic"), Array(("pp", "pp"), ("df", "df")))
    squidNode.routeTo(id, "oto".getBytes)
    squidNode.routeTo(new SquidId(1, 32, Array("numeric"), Array(("93361", "93361"))), "asdf".getBytes)
    squidNode.routeTo(new SquidId(1, 32, Array("alphabetic"), Array(("hola", "hola"))), "asdf".getBytes)
    squidNode.routeTo(new SquidId(1, 32, Array("numeric"), Array(("99993122", "99993122"))), "holaaa".getBytes)


    sys.exit
  }
}
