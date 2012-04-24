package virtualp2p.test

import virtualp2p.comet.Comet
import virtualp2p.common.XmlTuple

/**
 * User: alejandro
 * Date: 8/04/12
 * Time: 02:26 PM
 */

object TestComet {
  def receive(data : Array[Byte]){

  }

  def main(args: Array[String]){
    var comet : Comet = new Comet
    comet.join
    val header = <keys><id>1</id><zone>as</zone></keys>

    val tuple : XmlTuple = new XmlTuple(header, "hola".getBytes)

    comet.out(tuple)
    comet.register(TestComet.receive)

    comet.rd(tuple)

    //sys.exit
  }
}
