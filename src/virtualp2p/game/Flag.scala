package virtualp2p.game

import com.jme3.scene.shape.Box
import com.jme3.material.Material
import com.jme3.app.Application._
import com.jme3.asset.AssetManager
import com.jme3.math.{Transform, ColorRGBA, Vector3f}
import com.jme3.scene.{Spatial, Geometry}

/**
 * User: alejandro
 * Date: 1/05/12
 * Time: 09:21 PM
 */

class Flag(position : Vector3f, id : String, color : ColorRGBA, assetManager : AssetManager) extends GameObject(position, id, ObjectTypes.FLAG.toString){
  val box = new Box(Vector3f.ZERO, 1,1,1)
  val mat = new Material(assetManager, "Common/MatDefs/Misc/Unshaded.j3md")
  mat.setColor("Color", color)

  override var spatial = new Geometry("Box", box).asInstanceOf[Spatial]
  spatial.setMaterial(mat)
  spatial.setLocalScale(4.0f)

  override var pastTransforms = new Array[(Transform, Float)](1)

  def deadReckoning(tpf : Float){}
}
