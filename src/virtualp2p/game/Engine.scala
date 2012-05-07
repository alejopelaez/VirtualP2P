package virtualp2p.game

import virtualp2p.comet.Comet
import virtualp2p.common.{XmlTuple, Logger}

import scala.Array
import collection.mutable.ListBuffer

import com.jme3.app.SimpleApplication
import com.jme3.material.Material

import com.jme3.scene.shape.Box
import com.jme3.light.DirectionalLight
import com.jme3.input.controls.{AnalogListener, ActionListener, MouseButtonTrigger, KeyTrigger}
import com.jme3.math._

import java.io.{FileNotFoundException, FileInputStream}
import java.util.Properties
import com.jme3.input.{ChaseCamera, MouseInput, KeyInput}
import com.jme3.scene.control.CameraControl.ControlDirection
import com.jme3.texture.Texture
import com.jme3.texture.Texture.WrapMode
import com.jme3.terrain.geomipmap.{TerrainLodControl, TerrainQuad}
import com.jme3.terrain.heightmap.{RawHeightMap, HeightMap, ImageBasedHeightMap}
import com.jme3.scene.{Node, CameraNode, Spatial, Geometry}
import util.{Marshal, Random}

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
  var objects : ListBuffer[GameObject] = new ListBuffer[GameObject]()
  var avatar : GameObject = null
  var flag : GameObject = null
  var acumTime : Float = 0
  var updateTime : Float = 0.5f
  var properties : Properties = null
  var avatarSpeed = 75.0f
  var score = 0
  var ai = false

  val rand : Random = new Random()

  def loadProperties(){
    Logger.println("Loading properties...", "Engine")
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

  def rotate(value : Float){
    avatar.spatial.rotate(0, avatarSpeed * value * 0.1f, 0)
  }

  def walk(value : Float) {
    val f : Vector3f = avatar.spatial.getLocalRotation.getRotationColumn(2)
    val v : Vector3f = avatar.spatial.getLocalTranslation
    avatar.spatial.setLocalTranslation(v.add(f.mult(-avatarSpeed*value)))
  }

  /**
   * Init the key bindings
   */
  def initKeys(){

    val actionListener : ActionListener = new ActionListener {
      def onAction(name: String, isPressed: Boolean, tpf: Float) {
        name match {
          case "ai" => {
            if (!isPressed)
              ai = !ai
          }
          case _ => Logger.println("Engine: Unknown action " + name, "Engine")
        }
      }
    }

    val analogListener : AnalogListener = new AnalogListener {
      def onAnalog(name: String, value: Float, tpf: Float) {
        name match {
          case "Right" => rotate(-value)
          case "Left"  => rotate(value)
          case "Up"    => walk(value)
          case "Down"  => walk(-value)
          case _       => Logger.println("Engine: Unknown action " + name)
        }
      }
    }

    //Creates the mappings
    inputManager.addMapping("Pause",  new KeyTrigger(KeyInput.KEY_P))
    inputManager.addMapping("Left",   new KeyTrigger(KeyInput.KEY_A))
    inputManager.addMapping("Right",  new KeyTrigger(KeyInput.KEY_D))
    inputManager.addMapping("Up",     new KeyTrigger(KeyInput.KEY_W))
    inputManager.addMapping("Down",   new KeyTrigger(KeyInput.KEY_S))
    inputManager.addMapping("ai",     new KeyTrigger(KeyInput.KEY_T))

    new MouseButtonTrigger((MouseInput.BUTTON_LEFT))

    // Add the names to the action listener.
    inputManager.addListener(actionListener, "Pause", "Quit", "ai")
    inputManager.addListener(analogListener, "Left", "Right", "Up", "Down")
  }

  /**
   * Loads the terrain
   */
  def loadTerrain = {
    /** 1. Create terrain material and load four textures into it. */
    val mat_terrain = new Material(assetManager,
      "Common/MatDefs/Terrain/Terrain.j3md");

    /** 1.1) Add ALPHA map (for red-blue-green coded splat textures) */
    mat_terrain.setTexture("Alpha", assetManager.loadTexture(
      "Textures/Terrain/splat/alphamap.png"));

    /** 1.2) Add GRASS texture into the red layer (Tex1). */
    val grass : Texture = assetManager.loadTexture(
      "Textures/Terrain/splat/grass.jpg");
    grass.setWrap(WrapMode.Repeat);
    mat_terrain.setTexture("Tex1", grass);
    mat_terrain.setFloat("Tex1Scale", 64f);

    /** 1.3) Add DIRT texture into the green layer (Tex2) */
    val dirt : Texture = assetManager.loadTexture(
      "Textures/Terrain/splat/dirt.jpg");
    dirt.setWrap(WrapMode.Repeat);
    mat_terrain.setTexture("Tex2", dirt);
    mat_terrain.setFloat("Tex2Scale", 32f);

    /** 1.4) Add ROAD texture into the blue layer (Tex3) */
    val rock : Texture = assetManager.loadTexture(
      "Textures/Terrain/splat/road.jpg");
    rock.setWrap(WrapMode.Repeat);
    mat_terrain.setTexture("Tex3", rock);
    mat_terrain.setFloat("Tex3Scale", 128f);

    val patchSize = 65;
    val terrain = new TerrainQuad("my terrain", patchSize, 513, new Array[Float](512*512));

    /** 4. We give the terrain its material, position & scale it, and attach it. */
    terrain.setMaterial(mat_terrain);
    terrain.setLocalScale(2f, 1f, 2f);
    rootNode.attachChild(terrain);

    /** 5. The LOD (level of detail) depends on were the camera is: */
    val control : TerrainLodControl = new TerrainLodControl(terrain, getCamera());
    terrain.addControl(control);
  }

  /**
   * Performs some initial configuration.
   */
  override def simpleInitApp() {
    loadProperties()
    initKeys()
    loadTerrain

    setPauseOnLostFocus(false);
    flyCam.setEnabled(false);

    comet = new Comet
    comet.join() //Fundamental to have a connection to comet
    comet.register(receive)
    Logger.println("Started succesfully", "Engine")

    //Creates the avatar
    avatar = new Avatar(Vector3f.ZERO, rand.nextLong().toString, assetManager)
    avatar.attachCamera(cam)
    objects += avatar
    rootNode.attachChild(avatar.spatial)

    //Create flag
    flag = new Flag(Vector3f.ZERO, rand.nextLong().toString, ColorRGBA.Blue, assetManager)
    flag.generateRandomPosition(rand)
    objects += flag
    rootNode.attachChild(flag.spatial)

    // You must add a light to make the model visible
    var sun : DirectionalLight = new DirectionalLight();
    sun.setDirection(new Vector3f(70f, -70f, 70f));
    rootNode.addLight(sun);
    var sun2 : DirectionalLight = new DirectionalLight();
    sun.setDirection(new Vector3f(-70f, -70f, -70f));
    rootNode.addLight(sun2);
  }

  /**
   * Sends the current state to comet.
   */
  def sendState() {
    var id : String= avatar.id
    var typ : String = avatar.objectType
    var trans = avatar.spatial.getLocalTransform
    var updateMessage = new UpdateMessage(trans, id, typ)
    var header = <header><keys><type>1</type><zone>1</zone></keys><secondary><id>{id}</id></secondary></header>
    var tuple : XmlTuple = new XmlTuple(header, Marshal.dump(updateMessage))
    comet.out(tuple)

    id = flag.id
    typ = flag.objectType
    trans = flag.spatial.getLocalTransform
    updateMessage = new UpdateMessage(trans, id, typ)
    header = <header><keys><type>2</type><zone>1</zone></keys><secondary><id>{id}</id></secondary></header>
    tuple = new XmlTuple(header, Marshal.dump(updateMessage))
    comet.out(tuple)
  }

  /**
   * Gets the current state from comet.
   */
  def getState() {
    var header = <header><keys><type>1</type><zone>1</zone></keys><secondary><id>*</id></secondary></header>
    var tuple : XmlTuple = new XmlTuple(header, null)
    comet.rd(tuple)

    header = <header><keys><type>2</type><zone>1</zone></keys><secondary><id>*</id></secondary></header>
    tuple = new XmlTuple(header, null)
    comet.rd(tuple)
  }

  /**
   * Updates the player positions in the network.
   */
  def updateNetwork() {
    Logger.println("Sending state to comet", "Engine")
    sendState
    Logger.println("Getting state from comet", "Engine")
    getState()
  }

  /**
   * Process a pending update.
   * @param message The update to be processed
   */
  def processUpdate(message : UpdateMessage, tpf : Float) {
    var found = false
    objects.foreach(obj => {
      if (obj.objectType == message.objectType && obj.id == message.id){
        if (obj.id != avatar.id) obj.addTransform(message.transform, tpf)
        obj.spatial.setLocalTransform(message.transform)
        found = true
      }
    })
    if (!found){
      ObjectTypes.withName(message.objectType) match {
        case ObjectTypes.AVATAR =>
          Logger.println("Received new avatar with id " + message.id, "Engine")
          var av : Avatar = new Avatar(Vector3f.ZERO, message.id, assetManager)
          av.spatial.setLocalTransform(message.transform)
          objects += av
          rootNode.attachChild(av.spatial);
        case ObjectTypes.FLAG =>
          Logger.println("Received flag object with id " + message.id, "Engine")
          var fl = new Flag(Vector3f.ZERO, message.id, ColorRGBA.Red, assetManager)
          objects += fl
          rootNode.attachChild(fl.spatial)
        case _ => Logger.println("Engine: Unknown Object type " + message.objectType)
      }
    }
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

    // Process all pending update jobs
    val updates = UpdateQueue.pendingUpdates
    if (updates.size > 0) {
      Logger.println("Processing " + updates.size + " pending updates", "Engine")
      updates.foreach(update => {
        processUpdate(update, tpf)
      })
    }

    //Check if we captured the flag
    val distance = avatar.spatial.getLocalTranslation.distanceSquared(flag.spatial.getLocalTranslation)
    if(distance < 60){
      Logger.println("Captured the flag!!", "Engine")
      score += 1
      flag.generateRandomPosition(rand)
    }

    //AI
    if (ai){
      val flagPos : Vector3f = flag.spatial.getLocalTranslation
      val avatarPos : Vector3f = avatar.spatial.getLocalTranslation
      val dir = flagPos.subtract(avatarPos).normalize()
      val currDir = avatar.spatial.getLocalRotation.getRotationColumn(2).negate
      val mag = 1 - dir.dot(currDir)/(dir.length() * currDir.length())

      val cross = dir.cross(currDir)

      if (cross.y < 0)
        rotate(tpf*mag*1.5f)
      else
        rotate(-tpf*mag*1.5f)

      walk(tpf*1.2f)
    }

    //Perform dead reckoning on the avatars
    objects.foreach(obj => {
      if (obj.id != avatar.id && obj.objectType == ObjectTypes.AVATAR.toString){
        obj.deadReckoning(tpf, speed)
      }
    })
  }

  /**
   * Receives a message from comet
   */
  def receive(data : Array[Byte]) {
    val message = Marshal.load[UpdateMessage](data)
    UpdateQueue.queueUpdate(message)
  }

  /**
   * Exits the application
   */
  override def stop(){
    // TODO some cleanup and stuff
    Logger.println("Exiting", "Engine")
    super.stop()
    sys.exit()
  }
}
