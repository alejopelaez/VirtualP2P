package virtualp2p.game

import com.jme3.math.{Transform, Vector3f}
import com.jme3.renderer.Camera
import com.jme3.scene.control.CameraControl.ControlDirection
import com.jme3.scene.{Node, CameraNode, Spatial}
import util.Random


/**
 * User: alejandro
 * Date: 1/05/12
 * Time: 09:17 PM
 */

/**
 * Base class for all game objects
 */
abstract class GameObject(position : Vector3f, var id : String, var objectType : String){
  var spatial : Spatial
  var pastTransforms : Array[(Transform, Float)]

  def deadReckoning(tpf : Float, speed : Float)

  /**
   * Attachs a camera to this object
   * @param camera The camera to attach
   */
  def attachCamera(camera : Camera){
    val camNode = new CameraNode("Camera Node", camera);
    camNode.setControlDir(ControlDirection.SpatialToCamera);
    spatial.asInstanceOf[Node].attachChild(camNode);
    camNode.setLocalTranslation(new Vector3f(0, 250.0f, 280.0f));
    camNode.lookAt(spatial.getLocalTranslation().add(0, 9.0f, 0), Vector3f.UNIT_Y);
  }

  def generateRandomPosition(rand : Random){
    val x = (rand.nextFloat() - 0.5f)*800.0f
    val z = (rand.nextFloat() - 0.5f)*800.0f

    spatial.setLocalTranslation(x,4f,z)
  }

  /**
   * Adds a transform to the previos transform array
   * @param transform The transform to add
   */
  def addTransform(transform : Transform, tpf : Float) {
    val size = pastTransforms.size
    pastTransforms = pastTransforms.indices.toArray.map(i => pastTransforms((i+1)%size))
    pastTransforms(0) = (transform, tpf)
  }
}
