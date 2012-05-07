package virtualp2p.test

import virtualp2p.comet.Comet
import virtualp2p.common.XmlTuple
import util.Marshal
import java.util.Date

/**
 * User: alejandro
 * Date: 8/04/12
 * Time: 02:26 PM
 */

object TestComet {
  def receive(data : Array[Byte], date : Date){
    val message = Marshal.load[String](data)
    println(message)
  }

  def main(args: Array[String]){
    var comet : Comet = new Comet
    comet.join()
    val header = <keys><id>1</id><zone>as</zone></keys>

    val tuple : XmlTuple = new XmlTuple(header, Marshal.dump("Probando"))

    comet.out(tuple, new Date)
    comet.register(TestComet.receive)

    Thread.sleep(500)

    comet.rd(tuple, new Date)

    Thread.sleep(500)

    sys.exit()
  }
}
