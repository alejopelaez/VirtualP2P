package virtualp2p.game

import virtualp2p.comet.Comet
import virtualp2p.common.XmlTuple
import util.Marshal

import com.jme3.app.{SimpleApplication}

import com.jme3.material.{Material};
import com.jme3.math._;
import com.jme3.scene.shape.{Box}
import com.jme3.scene.{Spatial, Geometry}
import com.jme3.light.DirectionalLight

import scala.Array

import java.io.{FileNotFoundException, FileInputStream}
import java.util.Properties
import com.jme3.input.{MouseInput, KeyInput}
import com.jme3.input.controls.{AnalogListener, ActionListener, MouseButtonTrigger, KeyTrigger}

/**
 * User: alejandro
 * Date: 24/04/12
 * Time: 06:44 PM
 */

/**
 * Main class which handles the high level logic of the game.
 */
class Engine(file : String) extends SimpleApplication {
  def this() = this("config/engine.properties")

  var comet : Comet = null
  var avatar : Spatial = null
  var acumTime : Float = 0
  var updateTime : Float = 0.5f
  var properties : Properties = null
  var avatarSpeed = 25.0f

  def loadProperties(){
    println("Engine: loading properties...")
    //Load properties
    var input : FileInputStream = null
    try {
      input = new FileInputStream(file);
    } catch {
      case e : FileNotFoundException => {
        println("Engine constructor: " + e.getMessage)
        sys.exit()
      }
    }
    System.getProperties.load(input)
    properties = System.getProperties

    updateTime = properties.getProperty("updateTime", "0.5").toFloat
  }

  /**
   * Init the key bindings
   */
  def initKeys(){

    val actionListener : ActionListener = new ActionListener {
      def onAction(name: String, isPressed: Boolean, tpf: Float) {
        name match {
          case _ => println("Engine: Unknown action " + name)
        }
      }
    }

    val analogListener : AnalogListener = new AnalogListener {
      def onAnalog(name: String, value: Float, tpf: Float) {
        name match {
          case "Right" => avatar.rotate(0, avatarSpeed * value * 0.5f, 0)
          case "Left" => avatar.rotate(0, -avatarSpeed * value * 0.5f, 0)
          case "Up" => {
            val f : Vector3f = avatar.getLocalRotation.getRotationColumn(2)
            val v : Vector3f = avatar.getLocalTranslation()
            avatar.setLocalTranslation(v.add(f.mult(avatarSpeed*value).negate))
          }
          case "Down" => {
            val f : Vector3f = avatar.getLocalRotation.getRotationColumn(2)
            val v : Vector3f = avatar.getLocalTranslation()
            avatar.setLocalTranslation(v.add(f.mult(avatarSpeed*value)))
          }
          case _ => println("Engine: Unknown action " + name)
        }
      }
    }

    //Creates the mappings
    inputManager.addMapping("Pause",  new KeyTrigger(KeyInput.KEY_P))
    inputManager.addMapping("Left",   new KeyTrigger(KeyInput.KEY_A))
    inputManager.addMapping("Right",  new KeyTrigger(KeyInput.KEY_D))
    inputManager.addMapping("Up",   new KeyTrigger(KeyInput.KEY_W))
    inputManager.addMapping("Down",  new KeyTrigger(KeyInput.KEY_S))

    new MouseButtonTrigger((MouseInput.BUTTON_LEFT))

    // Add the names to the action listener.
    inputManager.addListener(actionListener, "Pause", "Quit")
    inputManager.addListener(analogListener, "Left", "Right", "Up", "Down")
  }

  /**
   * Performs some initial configuration.
   */
  override def simpleInitApp {
    loadProperties()
    initKeys()

    comet = new Comet
    comet.join //Fundamental to have a connection to comet
    comet.register(receive)
    println("Engine started succesfully")

    // Load a model from test_data (OgreXML + material + texture)
    avatar = assetManager.loadModel("Models/Ninja/Ninja.mesh.xml");
    avatar.scale(0.05f, 0.05f, 0.05f);
    avatar.rotate(0.0f, -3.0f, 0.0f);
    avatar.setLocalTranslation(0.0f, -5.0f, -2.0f);
    rootNode.attachChild(avatar);

    // You must add a light to make the model visible
    var sun : DirectionalLight = new DirectionalLight();
    sun.setDirection(new Vector3f(-0.1f, -0.7f, -1.0f));
    rootNode.addLight(sun);
  }

  /**
   * Updates the player positions in the network.
   */
   def updateNetwork() {
     println("Engine: Sending state to comet")

   }

  /**
   * The update loop.
   */
  override def simpleUpdate(tpf : Float) {
    acumTime += tpf
    if (acumTime > updateTime){
      updateNetwork()
      acumTime = 0
    }
  }

  /**
   * Receives a message from comet
   */
  def receive(data : Array[Byte]) {

  }

  /**
   * Exits the application
   */
  override def stop(){
    // TODO some cleanup and stuff
    println("Engine: Exiting")
    super.stop
    sys.exit()
  }
}
