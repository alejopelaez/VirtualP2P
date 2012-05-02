package virtualp2p.game

import com.jme3.scene.Node
import com.jme3.asset.AssetManager
import com.jme3.math.{Transform, Vector3f}

/**
 * User: alejandro
 * Date: 1/05/12
 * Time: 09:20 PM
 */

class Avatar(position : Vector3f, id : String, assetManager : AssetManager) extends GameObject(position, id, ObjectTypes.AVATAR.toString){
  override var spatial = assetManager.loadModel("Models/Ninja/Ninja.mesh.xml")
  spatial.scale(0.05f, 0.05f, 0.05f)

  override var pastTransforms = new Array[(Transform, Float)](3)

  def deadReckoning(tpf : Float){
    val tr1 = pastTransforms(0)
    val tr2 = pastTransforms(1)

    if (tr1 == null || tr2 == null) return

    val p1 = tr1._1.getTranslation
    val p2 = tr2._1.getTranslation

    val direction = p1.subtract(p2)

    if (direction.lengthSquared() < 0.0001f) return
    val newPos = spatial.getLocalTranslation.add(direction.mult(tpf))

    spatial.setLocalTranslation(newPos)
    //addTransform(spatial.getLocalTransform, tpf)
  }
}
