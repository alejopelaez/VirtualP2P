package virtualp2p.test

import virtualp2p.comet.Comet

/**
 * User: alejandro
 * Date: 8/04/12
 * Time: 02:26 PM
 */

object TestComet {
  def main(args: Array[String]){
    var comet : Comet = new Comet
    comet.join

    comet.in(null)

    //sys.exit
  }
}
