package virtualp2p.game

import com.jme3.scene.Spatial
import com.jme3.math.Transform

/**
 * User: alejandro
 * Date: 1/05/12
 * Time: 02:56 PM
 */

object ObjectTypes extends Enumeration {
  type ObjectTypes = Value
  val AVATAR = Value("AVATAR")
  val FLAG = Value("FLAG")
}

class UpdateMessage(val transform : Transform, val id : String, val objectType : String) extends Serializable{
}
