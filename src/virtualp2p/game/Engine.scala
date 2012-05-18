package virtualp2p.game

import virtualp2p.comet.Comet
import virtualp2p.common.{XmlTuple, Logger}

import scala.Array
import collection.mutable.ListBuffer

import com.jme3.app.SimpleApplication
import com.jme3.material.Material

import com.jme3.scene.shape.Box
import com.jme3.light.DirectionalLight

import com.jme3.input.{ChaseCamera, MouseInput, KeyInput}
import com.jme3.scene.control.CameraControl.ControlDirection
import com.jme3.texture.Texture
import com.jme3.texture.Texture.WrapMode
import com.jme3.terrain.geomipmap.{TerrainLodControl, TerrainQuad}
import com.jme3.terrain.heightmap.{RawHeightMap, HeightMap, ImageBasedHeightMap}
import com.jme3.scene.{Node, CameraNode, Spatial, Geometry}
import java.util.{Date, Properties}
import com.jme3.font.BitmapText
import com.jme3.system.AppSettings
import com.jme3.input.controls._
import com.jme3.math._
import util.{Marshal, Random}
import virtualp2p.meteor.Meteor
import java.io._

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

  //Some constants
  val AVATAR_TYPE = 1
  val FLAG_TYPE = 2
  val AREAS_ID = Array[Int](1, 2, 3, 4)
  var current_area = 1

  var comet : Comet = null
  var meteor : Meteor = null
  var objects : ListBuffer[GameObject] = new ListBuffer[GameObject]()
  var avatar : GameObject = null
  var flag : GameObject = null
  var acumTime : Float = 0
  var updateTime : Float = 0.5f
  var properties : Properties = null
  var avatarSpeed = 75.0f
  var score = 0
  var ai = false
  var backend = "comet"
  var started = false
  var runTime = 0.0f
  var fps = 0.0f

  //GUI texts
  var stat_text : BitmapText = null

  // Statistics
  var runningTime = 0.0f
  var takeStatistics = false
  var averageLatency = 0.0f
  var messagesReceived = 0
  var messagesSent = 0

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
    input.close
    properties = System.getProperties

    updateTime = properties.getProperty("updateTime", "0.5").toFloat
    backend = properties.getProperty("backend", "comet")
    runningTime = Integer.parseInt(properties.getProperty("runningTime", "120")).toFloat
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
            if (!isPressed){
              ai = !ai
              started = true
              takeStatistics = true
              runTime = 0.0f
              averageLatency = 0.0f
              messagesReceived = 0
              messagesSent = 0
              if (backend == "comet")comet.resetStatistics()
              if (backend == "meteor")meteor.resetStatistics()
            }
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

  def loadText(text : BitmapText, content : String, offset : Int) {
    text.setSize(guiFont.getCharSet().getRenderedSize());
    text.setColor(ColorRGBA.White);
    text.setText(content);
    text.setLocalTranslation(0, text.getLineHeight() + offset, 0);
    guiNode.attachChild(text);
  }

  /**
   * Performs some initial configuration.
   */
  override def simpleInitApp() {
    loadProperties()
    initKeys()
    loadTerrain

    // Load statistics text
    setDisplayStatView(false)
    stat_text = new BitmapText(guiFont, false);
    loadText(stat_text, "0", 145)

    setPauseOnLostFocus(false);
    flyCam.setEnabled(false);

    if (backend == "comet") {
      comet = new Comet
      comet.join() //Fundamental to have a connection to comet
      comet.register(receive)
      Logger.println("Started succesfully using comet backend", "Engine")
    } else {
      meteor = new Meteor
      meteor.join() //Fundamental to have a connection to comet
      meteor.register(receive)
      subscribe
      Logger.println("Started succesfully using meteor backend", "Engine")
    }

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
    var sun : DirectionalLight = new DirectionalLight()
    sun.setDirection(new Vector3f(70f, -70f, 70f))
    rootNode.addLight(sun)
    var sun2 : DirectionalLight = new DirectionalLight()
    sun.setDirection(new Vector3f(-70f, -70f, -70f))
    rootNode.addLight(sun2)
  }

  /**
   * Sends the current state to comet.
   */
  def sendState {
    var id : String= avatar.id
    var typ : String = avatar.objectType
    var trans = avatar.spatial.getLocalTransform
    var updateMessage = new UpdateMessage(trans, id, typ)
    var header = <header>
                    <keys><type>{AVATAR_TYPE}</type><zone>{current_area}</zone></keys>
                    <secondary><id>{id}</id></secondary>
                 </header>
    var tuple : XmlTuple = new XmlTuple(header, Marshal.dump(updateMessage))
    messagesSent += 1
    comet.out(tuple, new Date)

    id = flag.id
    typ = flag.objectType
    trans = flag.spatial.getLocalTransform
    updateMessage = new UpdateMessage(trans, id, typ)
    header = <header><keys><type>{FLAG_TYPE}</type><zone>1</zone></keys><secondary><id>{id}</id></secondary></header>
    tuple = new XmlTuple(header, Marshal.dump(updateMessage))
    messagesSent += 1
    comet.out(tuple, new Date)
  }

  /**
   * Gets the current state from comet.
   */
  def getState {
    AREAS_ID.foreach(area => {
      var header = <header><keys><type>{AVATAR_TYPE}</type><zone>{area}</zone></keys><secondary><id>*</id></secondary></header>
      var tuple : XmlTuple = new XmlTuple(header, null)
      messagesSent += 1
      comet.rd(tuple, new Date)
    })

    var header = <header><keys><type>{FLAG_TYPE}</type><zone>1</zone></keys><secondary><id>*</id></secondary></header>
    var tuple = new XmlTuple(header, null)
    messagesSent += 1
    comet.rd(tuple, new Date)
  }

  /**
   * Subscribe to the notifications for the current area.
   */
  def subscribe {
    AREAS_ID.foreach(area => {
      var header = <header><keys><type>{AVATAR_TYPE}</type><zone>{area}</zone></keys><secondary><id>*</id></secondary></header>
      var tuple : XmlTuple = new XmlTuple(header, null)
      messagesSent += 1
      meteor.subscribe(tuple, new Date)
    })

    var header = <header><keys><type>{FLAG_TYPE}</type><zone>1</zone></keys><secondary><id>*</id></secondary></header>
    var tuple = new XmlTuple(header, null)
    messagesSent += 1
    meteor.subscribe(tuple, new Date)
  }

  def unsubscribe {
    AREAS_ID.foreach(area => {
      var header = <header><keys><type>{AVATAR_TYPE}</type><zone>{area}</zone></keys><secondary><id>*</id></secondary></header>
      var tuple : XmlTuple = new XmlTuple(header, null)
      messagesSent += 1
      meteor.unsubscribe(tuple, new Date)
    })

    var header = <header><keys><type>{FLAG_TYPE}</type><zone>1</zone></keys><secondary><id>*</id></secondary></header>
    var tuple = new XmlTuple(header, null)
    messagesSent += 1
    meteor.unsubscribe(tuple, new Date)
  }

  /**
   * Publish the state to meteor
   */
  def publishState{
    var id : String= avatar.id
    var typ : String = avatar.objectType
    var trans = avatar.spatial.getLocalTransform
    var updateMessage = new UpdateMessage(trans, id, typ)
    var header = <header>
      <keys><type>{AVATAR_TYPE}</type><zone>{current_area}</zone></keys>
      <secondary><id>{id}</id></secondary>
    </header>
    var tuple : XmlTuple = new XmlTuple(header, Marshal.dump(updateMessage))
    messagesSent += 1
    meteor.publish(tuple, new Date)

    id = flag.id
    typ = flag.objectType
    trans = flag.spatial.getLocalTransform
    updateMessage = new UpdateMessage(trans, id, typ)
    header = <header><keys><type>{FLAG_TYPE}</type><zone>1</zone></keys><secondary><id>{id}</id></secondary></header>
    tuple = new XmlTuple(header, Marshal.dump(updateMessage))
    messagesSent += 1
    meteor.publish(tuple, new Date)
  }

  /**
   * Updates the player positions in the network.
   */
  def updateNetwork() {
    if (backend == "comet") {
      sendState
      getState
    } else {
      publishState
    }
  }

  /**
   * Process a pending update.
   * @param message The update to be processed
   */
  def processUpdate(message : UpdateMessage, tpf : Float) {
    var found = false
    objects.foreach(obj => {
      if (obj.objectType == message.objectType && obj.id == message.id){
        if (obj.id != avatar.id){
          obj.addTransform(message.transform, tpf)
          obj.spatial.setLocalTransform(message.transform)
        }
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

  def changeArea(target : Int){
    if (backend == "comet"){
      var header = <header><keys><type>{AVATAR_TYPE}</type><zone>{current_area}</zone></keys><secondary><id>{avatar.id}</id></secondary></header>
      var tuple : XmlTuple = new XmlTuple(header, null)
      messagesSent += 1
      //Erase the tuple from comet
      comet.in(tuple, new Date)
    } else {
      unsubscribe
      subscribe
    }

    current_area = target
    //Clear the objects
    objects.foreach(obj => {
      if (obj.id != avatar.id && obj.id != flag.id)
        rootNode.detachChild(obj.spatial)
    })
    objects.clear
    objects += avatar
    objects += flag
    Logger.println("Engine: Changing area to: " + current_area, "Engine")
  }

  def endSimulation(tpf : Float) {
    ai = false
    takeStatistics = false
    started = false
    //write the statistic to a file
    try{
      val base = "results"
      var suf = 0
      var filename = base + suf.toString
      var f = new File(filename)
      while(f.exists()){
        suf += 1
        filename = base + suf.toString
        f = new File(filename)
      }
      val fstream = new FileWriter(filename);
      val out = new BufferedWriter(fstream);

      var text = ""
      text += "Running time: " + runTime + "s\n"
      text += "Average latency: " + averageLatency + "ms\n"
      text += "Messages received: " + messagesReceived + "\n"
      text += "Messages sent: " + messagesSent + "\n"
      if (backend == "comet"){
        text += "Comet messages sent: " + comet.messagesSent + "\n"
        text += "Comet messages received: " + comet.messagesReceived + "\n"
        text += "Comet objects stored: " + comet.numberStored
      } else {
        text += "Meteor messages sent: " + meteor.messagesSent + "\n"
        text += "Meteor messages received: " + meteor.messagesReceived + "\n"
        text += "Meteor objects stored: " + meteor.numberStored
      }
      text += "\nFPS: " + fps
      out.write(text);
      //Close the output stream
      out.close();
    } catch {
      case
        e : FileNotFoundException => {
        println(e.getMessage)
        sys.exit()
      }
    }
    runTime = 0.0f
  }

  /**
   * The update loop.
   */
  override def simpleUpdate(tpf : Float) {
    fps = fps*0.999f + (1.0f/tpf)*0.001f

    if (started){
      runTime += tpf
      if (runTime > runningTime){
        endSimulation(tpf)
      }
    }

    acumTime += tpf
    if (acumTime > updateTime){
      //Check if we changed area
      val tmp = getArea(avatar)
      if (tmp != current_area){
        //subscribe
        changeArea(tmp)
      }
      updateNetwork()
      acumTime = 0
    }

    // Process all pending update jobs
    val updates = UpdateQueue.pendingUpdates
    if (updates.size > 0) {
      updates.foreach(update => {
        processUpdate(update, tpf)
      })
    }

    //Check if we captured the flag
    val distance = avatar.spatial.getLocalTranslation.distanceSquared(flag.spatial.getLocalTranslation)
    if(distance < 60){
      Logger.println("Captured the flag!", "Engine")
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

    //Update GUI statistics
    var text = ""
    text += "Running time: " + runTime + "s\n"
    text += "Average latency: " + averageLatency + "ms\n"
    text += "Messages received: " + messagesReceived + "\n"
    text += "Messages sent: " + messagesSent + "\n"
    if (backend == "comet"){
      text += "Comet messages sent: " + comet.messagesSent + "\n"
      text += "Comet messages received: " + comet.messagesReceived + "\n"
      text += "Comet objects stored: " + comet.numberStored
    } else {
      text += "Meteor messages sent: " + meteor.messagesSent + "\n"
      text += "Meteor messages received: " + meteor.messagesReceived + "\n"
      text += "Meteor objects stored: " + meteor.numberStored
    }
    if (takeStatistics)
      stat_text.setText(text)
  }

  def getArea(obj : GameObject) : Int = {
    val pos = obj.spatial.getLocalTranslation
    if (pos.x >= 0.0f && pos.z >= 0.0f){
      AREAS_ID(0)
    } else if (pos.x >= 0.0f && pos.z < 0.0f){
      AREAS_ID(1)
    }  else if (pos.x < 0.0f && pos.z < 0.0f){
      AREAS_ID(2)
    } else {
      AREAS_ID(3)
    }
  }

  /**
   * Receives a message from comet or meteor
   */
  def receive(data : Array[Byte], date : Date) {
    val message = Marshal.load[UpdateMessage](data)
    UpdateQueue.queueUpdate(message)

    //Take some statistics
    messagesReceived += 1
    val n = messagesReceived.toFloat
    val current : Date = new Date
    val diff = current.getTime - date.getTime
    averageLatency = averageLatency * (n - 1.0f)/n + diff.toFloat/n
  }

  /**
   * Exits the application
   */
  override def stop(){
    // TODO some cleanup and stuff (delete avatar and cube)
    Logger.println("Exiting", "Engine")
    Logger.println("Average Latency: " + averageLatency + "ms", "Engine")
    Logger.println("Messages Sent: " + messagesSent, "Engine")
    Logger.println("Messages received: " + messagesReceived, "Engine")
    super.stop()
    sys.exit()
  }

  /**
   * When clicking X button
   */
  override def destroy {
    super.destroy()
    stop()
  }
}
