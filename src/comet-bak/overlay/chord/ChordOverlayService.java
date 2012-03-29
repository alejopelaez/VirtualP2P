/*
 * Copyright (c) 2009, NSF Center for Autonomic Computing, Rutgers University
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided
 * that the following conditions are met:
 *
 * - Redistributions of source code must retain the above copyright notice, this list of conditions and
 * the following disclaimer.
 * - Redistributions in binary form must reproduce the above copyright notice, this list of conditions and
 * the following disclaimer in the documentation and/or other materials provided with the distribution.
 * - Neither the name of the NSF Center for Autonomic Computing, Rutgers University, nor the names of its
 * contributors may be used to endorse or promote products derived from this software without specific prior
 * written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 */

package tassl.automate.overlay.chord;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Hashtable;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import programming5.concurrent.ConditionVariable;
import programming5.io.Debug;
import programming5.io.FileHandler;
import programming5.io.InstrumentedThread;
import programming5.io.Serializer;
import programming5.net.MessageArrivedEvent;
import programming5.net.MessageArrivedListener;
import programming5.net.MessagingClient;
import programming5.net.NetworkException;
import programming5.net.sockets.ReliableUDPClient;
import programming5.net.sockets.UDPClient;
import tassl.automate.overlay.NodeInfo;
import tassl.automate.overlay.NodeNeighborhood;
import tassl.automate.overlay.OverlayListener;
import tassl.automate.overlay.OverlayMessageListener;
import tassl.automate.overlay.OverlayService;
import tassl.automate.overlay.OverlayStructureListener;
import tassl.automate.overlay.ApplicationState;
import tassl.automate.overlay.ResolveException;
import tassl.automate.overlay.replication.ReplicationAwareListener;
import tassl.automate.overlay.replication.ReplicationLayer;
import tassl.automate.overlay.management.LoadManager;
//import tassl.automate.util.DeadNetwork;
import tassl.automate.util.NATAdapter;
import tassl.automate.util.SSHClientAdapter;
import tassl.automate.util.TCPAdapter;
import tassl.automate.util.SSLAdapter;

/**
 * Implementation of an overlay service that follows the chord specification and uses a socket
 * connection as the underlying network infrastructure.
 * @author ahernandez
 */
public class ChordOverlayService implements OverlayService<ChordID>, MessageArrivedListener {

    // Load properties file
    static {
        String propertiesFileName = System.getProperty("ChordPropertiesFile", "chord.properties");
        Properties p = new Properties(System.getProperties());
        try {
            p.load(new FileInputStream(propertiesFileName));
            System.setProperties(p);
        }
        catch (FileNotFoundException fnf) {
            System.out.println("No Chord properties file");
        }
        catch (IOException ioe) {
            System.out.println("Bad Chord properties file");
        }
    }

    // Chord objects
    protected ChordID localID;
    protected URI localURI;
    protected FingerTable fingerTable;
    protected Hashtable<String, ChordNodeInfo> remoteSuccessors = new Hashtable<String, ChordNodeInfo>();
    protected ReplicationLayer replication = null;
    protected LoadManager loadManager = null;

    // Utility objects
    protected MessagingClient network;
    protected Timer timer = new Timer();
    protected Random randomGenerator = new Random(System.currentTimeMillis());
    protected Hashtable<String, OverlayMessageListener> messageSubscribers = new Hashtable<String, OverlayMessageListener>();
    protected Vector<OverlayStructureListener> eventSubscribers = new Vector<OverlayStructureListener>();
    protected Hashtable<String, String> propertyTable;
    protected ChordNodeInfo virtualParent = null;
    protected ChordOverlayService hostInstance = null;
    protected Vector<ChordNodeInfo> virtualChildren = new Vector<ChordNodeInfo>();
    protected Hashtable<ChordID, ChordOverlayService> hostedVirtualNodes = new Hashtable<ChordID, ChordOverlayService>();
    protected Hashtable<String, ChordID> joinCache = new Hashtable<String, ChordID>();
    protected ChordResolveCache resolveCache = new ChordResolveCache();

    // Enumeration constants
    protected static final int PREDECESSOR = NodeNeighborhood.PREDECESSOR;
    protected static final int SUCCESSOR = NodeNeighborhood.SUCCESSOR;
    protected final int REDUNDANT_SUCC_LENGTH;

    // Configuration fields and properties
    public static final String BROADCAST_ADDR = "//255.255.255.255:4444";
    protected String LOCAL_BOOTSTRAP;
    protected int RETRY_LIMIT;
    protected long RETRY_PERIOD;
    protected long STABILIZE_PERIOD;
    protected long FIX_FINGERS_PERIOD;
    protected boolean LOG_NETWORK;
    protected final long STARTUP_DELAY;
    protected final boolean USE_RESOLVE_CACHE;

    // Internal state fields
    private ConditionVariable joined = new ConditionVariable();
    private ConditionVariable joinReplyReceived = new ConditionVariable();
    private ConditionVariable successorKnown = new ConditionVariable();
    private ConditionVariable stabilizeComplete = new ConditionVariable();
    private ReentrantLock joinLock = new ReentrantLock();
    private final Hashtable<ChordID, Vector<ChordResolveRequest>> resolveRequests = new Hashtable<ChordID, Vector<ChordResolveRequest>>();
    private ChordNodeInfo failedSuccessor = null;
    private boolean leaving = false;
    private ConditionVariable pingReturned = null;
    private boolean replicationEnabled = false;
    private int maxReplications = 0;
    private int virtualDepth = 0;

    // State for network failure simulation
    private MessagingClient savedNetwork = null;

    /**
     * Creates an instance of the chord overlay service with default parameters or those contained in
     * the chord.properties file. The new instance is in an unjoined state, so that the join method
     * must be called before the functionality of the overlay is available
     */
    public ChordOverlayService() {
        propertyTable = null;
        LOCAL_BOOTSTRAP = getProperty("chord.LOCAL_BOOTSTRAP", BROADCAST_ADDR);
        RETRY_LIMIT = Integer.parseInt(getProperty("chord.RETRY_LIMIT", "3"));
        RETRY_PERIOD = Integer.parseInt(getProperty("chord.RETRY_PERIOD", "5000"));
        STABILIZE_PERIOD = Long.parseLong(getProperty("chord.STABILIZE_PERIOD", "1000"));
        FIX_FINGERS_PERIOD = Long.parseLong(getProperty("chord.FIX_FINGERS_PERIOD", "0"));
        STARTUP_DELAY = Long.parseLong(getProperty("chord.STARTUP_DELAY", "10000"));
        REDUNDANT_SUCC_LENGTH = Integer.parseInt(getProperty("chord.FT_REDUNDANCY", "1"));
        LOG_NETWORK = Boolean.parseBoolean(getProperty("chord.LOG_NETWORK", "FALSE"));
        USE_RESOLVE_CACHE = Boolean.parseBoolean(getProperty("chord.USE_RESOLVE_CACHE", "FALSE"));
        String debugSets = getProperty("chord.DEBUG", null);
        if (debugSets != null) {
            if (!debugSets.isEmpty()) {
                String[] setNames = debugSets.split(",");
                for (String setName : setNames) {
                    int prefix = setName.indexOf(".");
                    if (prefix >= 0) {
                        Debug.enable(setName);
                    }
                    else {
                        Debug.enable("chord." + setName);
                    }
                }
            }
            else {
                Debug.enable("chord");
            }
        }
    }

    /**
     * Creates an instance of the chord overlay service, overriding default parameters or those contained in
     * the chord.properties file with any contained in the given property table. The new instance is
     * in an unjoined state, so that the join method must be called before the functionality of the
     * overlay is available
     */
    public ChordOverlayService(Hashtable<String, String> myProperties) {
        propertyTable = myProperties;
        LOCAL_BOOTSTRAP = getProperty("chord.LOCAL_BOOTSTRAP", BROADCAST_ADDR);
        RETRY_LIMIT = Integer.parseInt(getProperty("chord.RETRY_LIMIT", "3"));
        RETRY_PERIOD = Integer.parseInt(getProperty("chord.RETRY_PERIOD", "5000"));
        STABILIZE_PERIOD = Long.parseLong(getProperty("chord.STABILIZE_PERIOD", "1000"));
        FIX_FINGERS_PERIOD = Long.parseLong(getProperty("chord.FIX_FINGERS_PERIOD", "0"));
        STARTUP_DELAY = Long.parseLong(getProperty("chord.STARTUP_DELAY", "10000"));
        REDUNDANT_SUCC_LENGTH = Integer.parseInt(getProperty("chord.FT_REDUNDANCY", "1"));
        LOG_NETWORK = Boolean.parseBoolean(getProperty("chord.LOG_NETWORK", "FALSE"));
        USE_RESOLVE_CACHE = Boolean.parseBoolean(getProperty("chord.USE_RESOLVE_CACHE", "FALSE"));
        String debugSets = getProperty("chord.DEBUG", null);
        if (debugSets != null) {
            if (!debugSets.isEmpty()) {
                String[] setNames = debugSets.split(",");
                for (String setName : setNames) {
                    int prefix = setName.indexOf(".");
                    if (prefix >= 0) {
                        Debug.enable(setName);
                    }
                    else {
                        Debug.enable("chord." + setName);
                    }
                }
            }
            else {
                Debug.enable("chord");
            }
        }
    }

    // Class specific methods for ring construction

    /**
     * Creates an instance of the chord overlay service with default parameters or those contained in
     * the chord.properties file. The new instance is initialized with the given node file, which
     * encodes a full finger table. Thus, the instance is created in a joined state, so that the join
     * method does not have to be called before the functionality of the overlay is available
     */
    public ChordOverlayService(byte[] nodeFile) throws IOException {
        // Initialize properties
        propertyTable = null;
        LOCAL_BOOTSTRAP = getProperty("chord.LOCAL_BOOTSTRAP", BROADCAST_ADDR);
        RETRY_LIMIT = Integer.parseInt(getProperty("chord.RETRY_LIMIT", "3"));
        RETRY_PERIOD = Integer.parseInt(getProperty("chord.RETRY_PERIOD", "5000"));
        STABILIZE_PERIOD = Long.parseLong(getProperty("chord.STABILIZE_PERIOD", "1000"));
        FIX_FINGERS_PERIOD = Long.parseLong(getProperty("chord.FIX_FINGERS_PERIOD", "0"));
        STARTUP_DELAY = Long.parseLong(getProperty("chord.STARTUP_DELAY", "10000"));
        REDUNDANT_SUCC_LENGTH = Integer.parseInt(getProperty("chord.FT_REDUNDANCY", "1"));
        LOG_NETWORK = Boolean.parseBoolean(getProperty("chord.LOG_NETWORK", "FALSE"));
        USE_RESOLVE_CACHE = Boolean.parseBoolean(getProperty("chord.USE_RESOLVE_CACHE", "FALSE"));
        String debugSets = getProperty("chord.DEBUG", null);
        if (debugSets != null) {
            if (!debugSets.isEmpty()) {
                String[] setNames = debugSets.split(",");
                for (String setName : setNames) {
                    int prefix = setName.indexOf(".");
                    if (prefix >= 0) {
                        Debug.enable(setName);
                    }
                    else {
                        Debug.enable("chord." + setName);
                    }
                }
            }
            else {
                Debug.enable("chord");
            }
        }
        // Decode file
        try {
            fingerTable = (FingerTable) Serializer.deserialize(nodeFile);
            localID = (ChordID) fingerTable.getLocalNode().nodeID;
            try {
                localURI = new URI(fingerTable.getLocalNode().nodeURI);
            } catch (URISyntaxException use) {
                Debug.printStackTrace(use, "chord");
            }
            try {
                network = createNetworkInstance();
                network.establishConnection();
                network.addListener(this);
            } catch (NetworkException ne) {
                throw new IOException(ne.getMessage());
            }
            this.virtualJoin(); // TODO: Consider saving remote state as well
            if (STABILIZE_PERIOD > 0) {
                timer.schedule(new StabilizeTask(), STARTUP_DELAY, STABILIZE_PERIOD);
            }
            if (FIX_FINGERS_PERIOD > 0) {
                timer.schedule(new FixFingersTask(), STARTUP_DELAY, FIX_FINGERS_PERIOD);
            }
            successorKnown.evaluateCondition(true);
            joined.evaluateCondition(true);
            if (Debug.isEnabled("chord")) fingerTable.print();
        }
        catch (ClassNotFoundException cnfe) {
            throw new IOException("ChordOverlayService: Invalid node file: " + cnfe.getMessage());
        }
    }

    /**
     * Creates an instance of the chord overlay service, overriding default parameters or those contained in
     * the chord.properties file with any contained in the given property table. The new instance is
     * initialized with the given node file, which encodes a full finger table. Thus, the instance is
     * created in a joined state, so that the join method does not have to be called before the
     * functionality of the overlay is available
     */
    public ChordOverlayService(byte[] nodeFile, Hashtable<String, String> myProperties) throws IOException {
        // Initialize properties
        propertyTable = myProperties;
        LOCAL_BOOTSTRAP = getProperty("chord.LOCAL_BOOTSTRAP", BROADCAST_ADDR);
        RETRY_LIMIT = Integer.parseInt(getProperty("chord.RETRY_LIMIT", "3"));
        RETRY_PERIOD = Integer.parseInt(getProperty("chord.RETRY_PERIOD", "5000"));
        STABILIZE_PERIOD = Long.parseLong(getProperty("chord.STABILIZE_PERIOD", "1000"));
        FIX_FINGERS_PERIOD = Long.parseLong(getProperty("chord.FIX_FINGERS_PERIOD", "0"));
        STARTUP_DELAY = Long.parseLong(getProperty("chord.STARTUP_DELAY", "10000"));
        REDUNDANT_SUCC_LENGTH = Integer.parseInt(getProperty("chord.FT_REDUNDANCY", "1"));
        LOG_NETWORK = Boolean.parseBoolean(getProperty("chord.LOG_NETWORK", "FALSE"));
        USE_RESOLVE_CACHE = Boolean.parseBoolean(getProperty("chord.USE_RESOLVE_CACHE", "FALSE"));
        String debugSets = getProperty("chord.DEBUG", null);
        if (debugSets != null) {
            if (!debugSets.isEmpty()) {
                String[] setNames = debugSets.split(",");
                for (String setName : setNames) {
                    int prefix = setName.indexOf(".");
                    if (prefix >= 0) {
                        Debug.enable(setName);
                    }
                    else {
                        Debug.enable("chord." + setName);
                    }
                }
            }
            else {
                Debug.enable("chord");
            }
        }
        // Decode file
        try {
            fingerTable = (FingerTable) Serializer.deserialize(nodeFile);
            localID = (ChordID) fingerTable.getLocalNode().nodeID;
            try {
                localURI = new URI(fingerTable.getLocalNode().nodeURI);
            } catch (URISyntaxException use) {
                Debug.printStackTrace(use, "chord");
            }
            try {
                network = createNetworkInstance();
                network.establishConnection();
                network.addListener(this);
            } catch (NetworkException ne) {
                throw new IOException(ne.getMessage());
            }
            this.virtualJoin(); // TODO: Consider saving remote state as well
            if (STABILIZE_PERIOD > 0) {
                timer.schedule(new StabilizeTask(), STARTUP_DELAY, STABILIZE_PERIOD);
            }
            if (FIX_FINGERS_PERIOD > 0) {
                timer.schedule(new FixFingersTask(), STARTUP_DELAY, FIX_FINGERS_PERIOD);
            }
            successorKnown.evaluateCondition(true);
            joined.evaluateCondition(true);
            if (Debug.isEnabled("chord")) fingerTable.print();
        }
        catch (ClassNotFoundException cnfe) {
            throw new IOException("ChordOverlayService: Invalid node file: " + cnfe.getMessage());
        }
    }

    /**
     * Creates an instance of the chord overlay service with default parameters or those contained in
     * the chord.properties file. The new instance is initialized with the given node file, which
     * encodes a full finger table. Thus, the instance is created in a joined state, so that the join
     * method does not have to be called before the functionality of the overlay is available
     */
    public ChordOverlayService(String filePath) throws IOException {
        // Initialize properties
        propertyTable = null;
        LOCAL_BOOTSTRAP = getProperty("chord.LOCAL_BOOTSTRAP", BROADCAST_ADDR);
        RETRY_LIMIT = Integer.parseInt(getProperty("chord.RETRY_LIMIT", "3"));
        RETRY_PERIOD = Integer.parseInt(getProperty("chord.RETRY_PERIOD", "5000"));
        STABILIZE_PERIOD = Long.parseLong(getProperty("chord.STABILIZE_PERIOD", "1000"));
        FIX_FINGERS_PERIOD = Long.parseLong(getProperty("chord.FIX_FINGERS_PERIOD", "0"));
        STARTUP_DELAY = Long.parseLong(getProperty("chord.STARTUP_DELAY", "10000"));
        REDUNDANT_SUCC_LENGTH = Integer.parseInt(getProperty("chord.FT_REDUNDANCY", "1"));
        LOG_NETWORK = Boolean.parseBoolean(getProperty("chord.LOG_NETWORK", "FALSE"));
        USE_RESOLVE_CACHE = Boolean.parseBoolean(getProperty("chord.USE_RESOLVE_CACHE", "FALSE"));
        String debugSets = getProperty("chord.DEBUG", null);
        if (debugSets != null) {
            if (!debugSets.isEmpty()) {
                String[] setNames = debugSets.split(",");
                for (String setName : setNames) {
                    int prefix = setName.indexOf(".");
                    if (prefix >= 0) {
                        Debug.enable(setName);
                    }
                    else {
                        Debug.enable("chord." + setName);
                    }
                }
            }
            else {
                Debug.enable("chord");
            }
        }
        // Load file
        FileHandler savedFile = new FileHandler(filePath, FileHandler.HandleMode.READ, FileHandler.FileType.BINARY);
        if (savedFile.getFileSize() > 0) {
            try {
                fingerTable = (FingerTable) Serializer.deserialize(savedFile.readFully());
                localID = (ChordID) fingerTable.getLocalNode().nodeID;
                try {
                    localURI = new URI(fingerTable.getLocalNode().nodeURI);
                } catch (URISyntaxException use) {
                    Debug.printStackTrace(use, "chord");
                }
                try {
                    network = createNetworkInstance();
                    network.establishConnection();
                    network.addListener(this);
                } catch (NetworkException ne) {
                    throw new IOException(ne.getMessage());
                }
                this.virtualJoin(); // TODO: Consider saving remote state as well
                if (STABILIZE_PERIOD > 0) {
                    timer.schedule(new StabilizeTask(), STARTUP_DELAY, STABILIZE_PERIOD);
                }
                if (FIX_FINGERS_PERIOD > 0) {
                    timer.schedule(new FixFingersTask(), STARTUP_DELAY, FIX_FINGERS_PERIOD);
                }
                successorKnown.evaluateCondition(true);
                joined.evaluateCondition(true);
                if (Debug.isEnabled("chord")) fingerTable.print();
            }
            catch (ClassNotFoundException ex) {
                throw new IOException("ChordOverlayService: Invalid node file: " + ex.getMessage());
            }
        }
        else {
            throw new IOException("ChordOverlayService: Could not load node file");
        }
    }

    /**
     * Encodes the state of the chord object (finger table) and saves it to disk, using the localID as
     * a file name.
     * @return the name of the saved chord file (extension .chn)
     */
    public String saveState() throws IOException {
        String saveName = localID.toString().concat(".chn");
        FileHandler saveFile = new FileHandler(saveName, FileHandler.HandleMode.OVERWRITE, FileHandler.FileType.BINARY);
        saveFile.write(Serializer.serialize(fingerTable));
        return saveName;
    }

    /**
     * Resets property table. If called after join, only properties that can change at runtime
     * are changed.
     * @param properties Property table
     */
    public void resetProperties(Hashtable<String, String> properties) {
        propertyTable = properties;
        LOCAL_BOOTSTRAP = getProperty("chord.LOCAL_BOOTSTRAP", BROADCAST_ADDR);
        RETRY_LIMIT = Integer.parseInt(getProperty("chord.RETRY_LIMIT", "3"));
        RETRY_PERIOD = Integer.parseInt(getProperty("chord.RETRY_PERIOD", "5000"));
        STABILIZE_PERIOD = Long.parseLong(getProperty("chord.STABILIZE_PERIOD", "1000"));
        FIX_FINGERS_PERIOD = Long.parseLong(getProperty("chord.FIX_FINGERS_PERIOD", "0"));
        LOG_NETWORK = Boolean.parseBoolean(getProperty("chord.LOG_NETWORK", "FALSE"));
    }

    public void enableNetworkLogging() {
        if (network != null) {
            if (network instanceof TCPAdapter) {
                ((TCPAdapter) network).turnLoggingOn("network_"+Integer.toString(localURI.getPort()));
            }
        }
    }

    public void disableNetworkLogging() {
        if (network != null) {
            if (network instanceof TCPAdapter) {
                ((TCPAdapter) network).turnLoggingOff();
            }
        }
    }

    public void setDebugEnabled(boolean enable, String... setNames) {
        if (setNames.length > 0) {
            for (String name : setNames) {
                Debug.setEnabled(name, enable);
            }
        }
        else {
            Debug.setEnabled("chord", enable);
        }
    }

    // Start implementation of OverlayService interface methods

    /**
     * Informs the overlay that the application state has changed externally (by events other
     * than messages received through the overlay), or because the application is handling all events. A
     * replication aware listener should have previously subscribed for the given tag; otherwise, calling
     * this method will have no effect.
     * @param tag the type of messages that the listener wishes to receive
     * @param stateObject an updated copy (snapshot) of the current application state
     */
    @Override
    public void updateApplicationState(String tag, ApplicationState stateObject) {
        RuntimeException re = new RuntimeException("ChordOverlayService: Cannot update application state: Not replicating for " + tag);
        if (replication != null) {
            if (replication.getLocalStateObject(tag) != null) {
                replication.replicateState(tag, stateObject);
            }
            else {
                throw re;
            }
        }
        else {
            throw re;
        }
    }

    @Override
    public ChordID join(String uri) throws IOException {
        try {
            if (!joined.isTrue()) {	// Only joins once
                String fixedIDValue = getProperty("chord.LOCAL_ID", null);
                if (fixedIDValue == null) {
                    localID = new ChordID(Integer.parseInt(getProperty("chord.ID_BITS", "160")));	// Creates random ID (TODO: Use hashing)
                    System.out.println("Join ID: " + localID);
//                    localID.setCluster(getProperty("chord.LOCAL_CLUSTER", ChordID.LOCAL_CLUSTER));
                }
                else {
                    localID = new ChordID(new BigInteger(fixedIDValue), Integer.parseInt(getProperty("chord.ID_BITS", "160")));
                }
                // Create URI object with absolute host address
                try {
                    localURI = new URI(fixURI(uri));
                }
                catch (Exception use) {
                    localURI = new URI(fixURI(getProperty("chord.LOCAL_URI", null)));
                }
                network = createNetworkInstance();
                if (localURI.getPort() < 0) {
                    localURI = new URI("//" + localURI.getHost() + ":" + ((TCPAdapter) network).getLocalPort());
                }
                network.addListener(this);
                network.establishConnection();
                byte[] joinMessage;
                URI bootstrapNode = new URI(fixURI(LOCAL_BOOTSTRAP));
                if (!bootstrapNode.equals(localURI)) {
                    System.out.println("Joining with bootstrap " + bootstrapNode);
                    joinMessage = ChordMessage.newJoinMessage(localID, localURI.toString(), (LOCAL_BOOTSTRAP.equals(BROADCAST_ADDR))).serialize();
                    // Send join message and wait for the reply, retrying if necessary
                    int retries = RETRY_LIMIT;
                    do {
                        try {
                            network.send(joinMessage, bootstrapNode.toString());
                        }
                        catch (NetworkException ne) {
                            Debug.printStackTrace(ne, "chord");
                        }
                        if (!joinReplyReceived.awaitUninterruptibly(RETRY_PERIOD, TimeUnit.MILLISECONDS)) {
                            System.out.println(localID + ": Did not get join response; retrying...");
                            retries--;
                        }
                    } while (!joinReplyReceived.isTrue() && retries > 0);

                    if (!joinReplyReceived.isTrue()) {	// Joined will be true if a reply was received
                        // If a reply wasn't received, assumes it is the first to join
                        fingerTable = new FingerTable(localID, localURI.toString(), REDUNDANT_SUCC_LENGTH);
                        joined.evaluateCondition(true);
                        System.out.println("No bootstrap node: Joining as first");
                    }
                    else {
                        joined.awaitUninterruptibly();
                        localID = fingerTable.localNode.getChordID();
                    }

                }
                else {
                    System.out.println("Joining as bootstrap");
                    fingerTable = new FingerTable(localID, localURI.toString(), REDUNDANT_SUCC_LENGTH);
                    joined.evaluateCondition(true);
                }

                successorKnown.evaluateCondition(true);

                // Obtain references to successors in remote clusters, if any
                this.virtualJoin();

                // Schedule self-healing tasks, unless disabled in properties file by setting period <= 0
                if (STABILIZE_PERIOD > 0) {
                    timer.schedule(new StabilizeTask(), STABILIZE_PERIOD, STABILIZE_PERIOD);
                }
                if (FIX_FINGERS_PERIOD > 0) {
                    timer.schedule(new FixFingersTask(), FIX_FINGERS_PERIOD, FIX_FINGERS_PERIOD);
                }

            }
        }
        catch (URISyntaxException use) {
            throw new IllegalArgumentException("ChordOverlayService: Cannot complete join: Bad URI given");
        }
        catch (NetworkException ne) {
            throw new IOException(ne.getMessage());
        }
        return localID;
    }

    @Override
    public void leave() {
        if (joined.isTrue()) {
            //>>for measuring the overhead by hjkim
            long stime, etime;
            stime = System.nanoTime() / 1000;
            //<<

            leaving = true;	// Modifies the behavior of resolution and routing methods
            timer.cancel();

            // Notify successor and predecessor of departure
            try {
                byte[] message = ChordMessage.newLeaveMessage(localID, fingerTable.successor()).serialize();
                network.send(message, fingerTable.predecessor().nodeURI);
                message = ChordMessage.newLeaveMessage(localID, fingerTable.predecessor()).serialize();
                network.send(message, fingerTable.successor().nodeURI);
                if (virtualParent != null) {
                    message = ChordMessage.newVirtualLeaveUpstreamMessage(fingerTable.localNode, virtualChildren).serialize();
                    network.send(message, virtualParent.nodeURI);
                }
                for (ChordNodeInfo virtualChild : virtualChildren) {
                    message = ChordMessage.newVirtualLeaveDownstreamMessage(fingerTable.localNode, virtualParent).serialize();
                    network.send(message, virtualChild.nodeURI);
                }
                if (hostInstance != null) {
                    hostInstance.hostedVirtualNodes.remove(localID);
                }
            }
            catch (Exception e) {
                Debug.printStackTrace(e, "chord");
            }
            finally {
                // Notify listeners of departure
                notifyDeparture();

                // TODO: Determine if hosted nodes should leave as well.

                // Close network connection, unsubscribe
    //            network.removeListener(this);
                network.endConnection();
                localID = null;
                joined.evaluateCondition(false);
            }
        }
        else {
            throw new RuntimeException("ChordOverlayService: Cannot perform leave: Not joined");
        }
    }

    public void terminate() {
        if (joined.isTrue()) {
            leaving = true;	// Modifies the behavior of resolution and routing methods
            timer.cancel();

            // Notify listeners of departure
            notifyTerminate();

            // TODO: Determine if hosted nodes should leave as well.

            // Close network connection, unsubscribe
//            network.removeListener(this);
            network.endConnection();
            localID = null;
            joined.evaluateCondition(false);
        }
        else {
            throw new RuntimeException("ChordOverlayService: Cannot perform terminmate: Not joined");
        }
    }

    @Override
    public ChordID generateID(Object... parameters) {
        IllegalArgumentException iae = new IllegalArgumentException("ChordOverlayService: Could not generate ID: Parameters: [BigInteger]");
        waitUntilJoined("generateID");
        ChordID ret = null;
        try {
            if (parameters.length == 0) {
                ret = new ChordID(localID.NUM_BITS);
            }
            else if (parameters.length == 1) {
                ret = new ChordID((BigInteger) parameters[0], localID.NUM_BITS);
            }
            else {
                throw iae;
            }
        }
        catch (ClassCastException cce) {
            throw iae;
        }
        return ret;
    }

    @Override
    /**
     * A ChordID is resolved by relaying a resolve request to the closest preceding finger to the ID in each node's finger table,
     * until the ID's successor is identified. Without failures, the request is only answered by the last node, directly to the
     * node where the query was issued; with failures, the reply won't be received, and a failsafe mode is used in which each node
     * checks the link before relaying the request, using alternative fingers whenever a failure is discovered.
     */
    public NodeInfo[] resolve(ChordID chordPeer) throws ResolveException {
        NodeInfo[] ret = null;
        if (joined.isTrue()) {
            // Check cache if enabled
            if (USE_RESOLVE_CACHE) {
                ChordNodeInfo cachedResolution = resolveCache.findResolution(chordPeer);
                if (cachedResolution != null) {
                    // Cache hit
                    ret = new NodeInfo[1];
                    ret[0] = cachedResolution;
                }
            }
            if (ret == null) {  // Cache miss or cache disabled
                ChordResolveRequest request = new ChordResolveRequest(chordPeer.isOfAllClusters() ? remoteSuccessors.size()+1 : 1);
                // Relay the resolve request and wait for response
                Vector<ChordResolveRequest> requests;
                int retries = RETRY_LIMIT;
                int turn;
                synchronized (resolveRequests) {
                    requests = resolveRequests.get(chordPeer);
                    if (requests == null) {
                        requests = new Vector<ChordResolveRequest>();
                        resolveRequests.put(chordPeer, requests);
                    }
                    requests.add(request);
                    turn = requests.size();
                }
                while (!request.isDone() && retries > 0) {
                    // Send request
                    synchronized (resolveRequests) {
                        if (turn == requests.size() && (turn == 1 || retries == RETRY_LIMIT)) {
                            this.relayResolveRequest(chordPeer, localURI.toString(), false /*(retries == 1)*/);   // Enter failsafe mode if last retry
                        }
                    }
                    // Wait for response (blocking operation)
                    if (!request.waitOn(RETRY_PERIOD, TimeUnit.MILLISECONDS)) {
                        retries--;
                    }
                }
                synchronized (resolveRequests) {
                    if (!request.isDone()) {
                        throw new ResolveException("ChordOverlayService", "Did not get reply from network");
                    }
                }
                ret = request.getResult();
                if (USE_RESOLVE_CACHE) {
                    resolveCache.insertResolvePair(chordPeer, (ChordNodeInfo) ret[0]);
                }
            }
        }
        else {
            throw new RuntimeException("ChordOverlayService: Cannot complete resolve: Not joined");
        }
        return ret;
    }

    @Override
    public void routeTo(ChordID peer, String tag, byte[] payload) {
        // Use same procedure as for multiple routing
        ChordIDCluster cluster = new ChordIDCluster(localID.NUM_BITS);
        cluster.add(peer);
        this.routeTo(cluster, tag, payload);
    }

    /**
     * Chord service routes a message to a list of peer IDs, which are ordered (order enforced by ChordIDCluster class) by resolving
     * the node responsible for the minimum ID in the list. A single message is sent to this node, which may be responsible for
     * several IDs in the list. If so, all IDs between the minimum and the resolved node are discarded, and the procedure is repeated
     * to send the message to the remaining IDs in the list, which will have a new minimum.
     */
    @Override
    public void routeTo(List<ChordID> peers, String tag, byte[] payload) {
        if (joined.isTrue()) {
            if (!peers.isEmpty()) {
                ChordIDCluster idCluster;
                try {   // Make sure the input is a list of ChordIDs (ChordIDCluster)
                    idCluster = (ChordIDCluster) peers;
                }
                catch (ClassCastException cce) {
                    idCluster = new ChordIDCluster(peers);
                }
                ChordID min = idCluster.elementAt(0);
                String refCluster = min.getCluster();
                for (ChordID id : peers) {
                    if (!id.getCluster().equals(refCluster)) {
                        throw new RuntimeException("ChordOverlayService: Cannot route to given cluster: Cluster ids must match for this method");
                    }
                    if (id.compareTo(min) < 0) {
                        min = id;
                    }
                }
                if (min.isOfAllClusters()) {    // Relay message to other rings if in two-level overlay
                    try {
                        NodeInfo[] destinations = this.resolve(min);
                        for (ChordID id : idCluster) {
                            id.setCluster(ChordID.LOCAL_CLUSTER);
                        }
                        for (NodeInfo node : destinations) {
                            byte[] message = ChordMessage.newRouteToMessage(idCluster, fingerTable.localNode, tag, payload, false, new Vector()).serialize();
                            robustSend(message, (ChordNodeInfo) node, false);
                        }
                    }
                    catch (ResolveException re) {

                    }
                }
                else {
                    this.relayRouteRequest(idCluster, fingerTable.localNode, tag, payload, false, new Vector());
                }
            }
        }
        else {
            throw new RuntimeException("ChordOverlayService: Unable to route message: Not joined");
        }
    }

    /**
     * Sends a message bypassing the overlay's routing protocol; used when a destination node address is known
     */
    @Override
    public void sendDirect(NodeInfo node, String tag, byte[] payload) throws IOException {
        ChordIDCluster dest = new ChordIDCluster(localID.NUM_BITS);
        if (node.nodeID != null) {
            dest.add((ChordID) node.nodeID);
        }
        byte[] message = ChordMessage.newRouteToMessage(dest, fingerTable.localNode, tag, payload, false, new Vector()).serialize();
        try {
            Debug.println("reply sent at chord ...","comet");
            if (!node.nodeURI.equals(localURI.toString())) {
                network.send(message, node.nodeURI);
            }
            else {
                this.signalEvent(new MessageArrivedEvent(message));
            }
        }
        catch (Exception e) {
            Debug.printStackTrace(e, "chord");
            throw new IOException("ChordOverlayService: Could not send message: " + e.getMessage());
        }
    }

    public void virtualGroupDownstreamSend(String tag, byte[] payload) throws IOException {
        for (ChordNodeInfo child : virtualChildren) {
            sendDirect(child, tag, payload);
        }
    }

    public void virtualGroupUpstreamSend(String tag, byte[] payload) throws IOException {
        sendDirect(virtualParent, tag, payload);
    }

    /**
     * Regular routing methods provides reliability by failsafe mode of resolve method
     */
    @Override
    public void reliableRouteTo(ChordID peer, String tag, byte[] payload) throws IOException {
        this.routeTo(peer, tag, payload);
    }

    /**
     * Regular routing methods provides reliability by failsafe mode of resolve method
     */
    @Override
    public void reliableRouteTo(List<ChordID> peers, String tag, byte[] payload) throws IOException {
        this.routeTo(peers, tag, payload);
    }

    /**
     * An overlay listener is notified of both messages received by chord and structural events in the
     * overlay
     * @param listener the object to be notified of messages and events
     * @param tag the types of messages the listener wishes to receive
     */
    @Override
    public void subscribe(OverlayListener listener, String tag) {
        this.subscribeToMessages(listener, tag);
        this.subscribeToEvents(listener);
    }

    @Override
    public void unsubscribe(OverlayListener listener, String tag) {
        this.unsubscribeFromMessages(listener, tag);
        this.unsubscribeFromEvents(listener);
    }

    /**
     * An overlay structure listener is notified only of structural events in the overlay (joins, departures,
     * failures)
     * @param listener the object to be notified of overlay events
     */
    @Override
    public void subscribeToEvents(OverlayStructureListener listener) {
        eventSubscribers.add(listener);
    }

    /**
     * An overlay message listener is notified of messages sent with the given tag
     * @param listener the object to be notified of overlay messages
     * @param tag the type of message the listener wishes to receive
     */
    @Override
    public void subscribeToMessages(OverlayMessageListener listener, String tag) {
        messageSubscribers.put(tag, listener);
    }

    /**
     * A replication aware listener responds to events that cause replicated data to be used (recovered)
     * after node failure. In addition of being notified of messages (not events in this case), the
     * listener is notified when the local application state is merged with a replicated (remote)
     * application state. Note that the application state object is provided by the listener, so that
     * it can be copied and updated simultaneously in replicated copies. Because the listener subscribes
     * to messages, all events are handled by the replication layer directly (every event is replicated
     * automatically).
     * @param listener the object to be notified of messages and replication events
     * @param tag the type of message that the listener wishes to receive
     * @param stateObject the application state object that contains the logic to update the state of the
     * application with different overlay messages
     */
    @Override
    public void subscribeToMessages(ReplicationAwareListener listener, String tag, ApplicationState stateObject) {
        if (replication == null) {
            replication = new ReplicationLayer(this, Integer.parseInt(getProperty("overlay.replication.NUM_REPLICAS", "1")));
            this.subscribeToEvents(replication);
        }
        replication.replicateFor(tag, listener, stateObject, true);
        this.subscribeToMessages(replication, tag);
    }

    /**
     * A replication aware listener responds to events that cause replicated data to be used (recovered)
     * after node failure. In addition of being notified of messages and structural events, the
     * listener is notified when the local application state is merged with a replicated (remote)
     * application state. Note that the application state object is provided by the listener, but it only
     * acts as a snapshot of a state at a given time. Because the listener subscribes to both events and
     * to messages, it must directly update its local application state and signal the overlay when the
     * state must be replicated. The state is merged (recovered) automatically upon node failure.
     * @param listener the object to be notified of messages and replication events
     * @param tag the type of message that the listener wishes to receive
     * @param stateObject a copy (snapshot) of the current application state, to be replicated by the
     * replication layer
     */
    @Override
    public void subscribe(ReplicationAwareListener listener, String tag, ApplicationState stateObject) {
        if (replication == null) {
            replication = new ReplicationLayer(this, Integer.parseInt(getProperty("overlay.replication.NUM_REPLICAS", "1")));
            this.subscribeToEvents(replication);
        }
        replication.replicateFor(tag, listener, stateObject, false);
        this.subscribeToMessages(listener, tag);
    }

    @Override
    public void unsubscribeFromEvents(OverlayStructureListener listener) {
        eventSubscribers.remove(listener);
    }

    @Override
    public void unsubscribeFromMessages(OverlayMessageListener listener, String tag) {
        messageSubscribers.remove(tag);
    }

    /**
     * The predecessor can be retrieved by getNeighborSet()[ChordOverlayService.PREDECESSOR]<p>
     * The successor can be retrieved by getNeighborSet()[ChordOverlayService.SUCCESSOR]<p>
     * These are the first two entries in the array. The rest of entries correspond to all other nodes
     * in the finger table and successors in other clusters, in no particular order.
     */
    @Override
    public NodeInfo[] getNeighborSet() {
        NodeInfo[] ret = null;
        if (joined.isTrue()) {
            if (!successorKnown.isTrue()) {
                successorKnown.awaitUninterruptibly();
            }
            ret = fingerTable.getKnownNodeInfo();
        }
        return ret;
    }

    @Override
    public NodeNeighborhood getNeighborhood() {
        if (!successorKnown.isTrue()) {
            successorKnown.awaitUninterruptibly();
        }
        ChordNodeNeighborhood ret = new ChordNodeNeighborhood(fingerTable.predecessor(), fingerTable.successor(), remoteSuccessors);
        return ret;
    }

    /**
     * @return the local ID if the node is in the joined state
     */
    @Override
    public NodeInfo getLocalID() {
        waitUntilJoined("getLocalID");
        return fingerTable.localNode;
    }

    /**
     * Not functional
     */
    @Override
    public void attachLoadManager(LoadManager managerRef) {
        loadManager = managerRef;
    }

    @Override
    public void createVirtualNode(ChordID nodeID) {
        if (joined.isTrue()) {
            ChordNodeInfo destination = selectPossibleHost(nodeID);
            if (!destination.nodeURI.equals(localURI.toString())) {
                robustSend(ChordMessage.newCreateVirtualNodeMessage(nodeID, fingerTable.localNode, 1).serialize(), destination, false);
            }
            else {
                this.processCreateVirtual(nodeID, fingerTable.localNode, 1);
            }
        }
        else {
            throw new RuntimeException("ChordOverlayService: Cannot create virtual node: Not joined");
        }
    }

    // Implementation of the DatagramListener interface
    @Override
    public void signalEvent(MessageArrivedEvent event) {
        final ChordMessage msg = (ChordMessage) ChordMessage.createFromBytes(event.getContentBytes());
        Debug.println("Received message " + msg.TYPE, "chord.SendDebug");
        if (Debug.isEnabled("chord.debugThreadTiming")) {
            final ChordOverlayService chordRef = this;
            new InstrumentedThread("ChordMessage-" + Integer.toString(msg.TYPE) + "_" + Long.toString(randomGenerator.nextLong())) {
                public void run() {
                    msg.applyTo(chordRef);
                }
            }.start();
        }
        else {
            msg.applyTo(this);
        }
    }

//    public void setEventReplicationEnabled(boolean enabled, int numNeighbors) {
//        replicationEnabled = enabled;
//        maxReplications = numNeighbors;
//    }

    /*
    public void simulateNetworkFailure() {
        network.removeListener(this);
        savedNetwork = network;
        network = new DeadNetwork();
    }
     */

    public void recoverNetwork() {
        if (savedNetwork != null) {
            network = savedNetwork;
            network.addListener(this);
            savedNetwork = null;
        }
    }

    // Start support methods for message handling

    protected void processJoinRequest(ChordID newNode, String uri, boolean broadcast) {
        try {
            if (Debug.isEnabled("chord.debugThreadTiming")) InstrumentedThread.startInvocation("processJoinRequest");
            Debug.println("Process join request called by " + newNode + " for " + uri + " at " + localURI, "chord.JoinDebug");
            joined.awaitUninterruptibly();
            if (joined.isTrue() && !leaving) {
                ChordMessage message = null;
                int neighborJoining = -1;
                boolean retry = false;
                ChordNodeInfo requestingNode = new ChordNodeInfo(newNode, uri);
                ChordNodeInfo destination = null;
                if (joinCache.containsKey(uri)) {
                    ChordID cachedID = joinCache.get(uri);
                    message = ChordMessage.newJoinAcceptMessage(fingerTable, cachedID);
                    destination = new ChordNodeInfo(cachedID, uri);
                }
                else {
                    ChordNodeInfo nextHop = fingerTable.closestPrecedingFinger(newNode);
                    if (nextHop == null) {
                        if (!requestingNode.equals(fingerTable.predecessor())) {
                            ChordID newID = splitDomain();
                            if (newID != null) {
                                Debug.println("Handling duplicate join ID (predecessor) " + newNode + " to " + newID, "chord.JoinDebug");
                                nextHop = fingerTable.predecessor();
                                requestingNode = new ChordNodeInfo(newID, uri);
                                newNode = newID;
                            }
                            else {
                                nextHop = fingerTable.successor();
                                newNode = localID;
                            }
                        }
                        else {
                            nextHop = fingerTable.predecessor();
                            retry = true;
                        }
                    }
                    if (nextHop.nodeID.equals(fingerTable.predecessor().nodeID) /*&& !realPredecessor*/) {
                        // This is the successor of the joining id, so must handle join
                        boolean recheck = false;
                        if (joinLock.isLocked()) {
                            recheck = true;
                        }
                        joinLock.lock();
                        if (!recheck) {
                            neighborJoining = (retry) ? -1 : PREDECESSOR;
                            if (!newNode.equals(localID)) {
                                // Reply with finger table to initialize new node
                                Debug.println("Joining node: " + newNode, "chord.JoinDebug");
                                message = ChordMessage.newJoinAcceptMessage(fingerTable, newNode);
                                destination = requestingNode;
                                fingerTable.update(requestingNode);
                                joinCache.put(uri, newNode);
                            }
                            else {
                                if (!uri.equals(localURI.toString())) {
                                    // Handle duplicate ID
                                    ChordID newID = splitDomain();
                                    if (newID != null) {
                                        Debug.println("Handling duplicate join ID (localnode)" + newNode + " to " + newID, "chord.JoinDebug");
                                        message = ChordMessage.newJoinAcceptMessage(fingerTable, newID);
                                        requestingNode = new ChordNodeInfo(newID, uri);
                                        destination = requestingNode;
                                        fingerTable.update(requestingNode);
                                        joinCache.put(uri, newID);
                                    }
                                    else {
                                        message = ChordMessage.newJoinMessage(localID, uri, false);
                                        destination = fingerTable.successor();
                                        neighborJoining = -1;
                                    }
                                }
                                else {
                                    destination = null;
                                }
                            }
                        }
                        else {
                            lockedHandleJoinRequest(newNode, uri);
                            destination = null;
                            neighborJoining = -1;
                        }
                    }
                    else if (!broadcast){
                        // This is not the successor, so pass the join request on, unless joining using broadcast
                        // Get closest preceding finger from finger table and send request on
                        message = ChordMessage.newJoinMessage(newNode, uri, false);
                        if (!nextHop.getChordID().equals(localID)) {
                            destination = nextHop;
                        }
                        else {
                            destination = fingerTable.successor();
                            neighborJoining = SUCCESSOR;
                        }
                    }
                }

                if (destination != null) {
                    if (message.TYPE == 1) {
                        Debug.println("Sending join request on to " + destination, "chord.JoinDebug");
                    }
                    else if (message.TYPE == -1) {
                        Debug.println("Sending join reply to " + destination, "chord.JoinDebug");
                    }
                    robustSend(message.serialize(), destination, (neighborJoining != PREDECESSOR));    // Enable forwarding unless sending a response to the joining predecessor
                }
//                if (requestingNode != null) {     // TODO: See if inclusive updating can be recovered
//                    fingerTable.update(requestingNode);
//                }
                if (neighborJoining >= 0) {
                    this.notifyJoin(neighborJoining);
                }
                Debug.println("Join exit for " + requestingNode.nodeID + " at " + uri, "chord.JoinDebug");
            }
        }
        catch (Exception e) {
            Debug.printStackTrace(e, "chord");
        }
        finally {
            if (joinLock.isLocked() && joinLock.isHeldByCurrentThread()) {
                joinLock.unlock();
            }
            if (Debug.isEnabled("chord.debugThreadTiming")) InstrumentedThread.endInvocation("processJoinRequest");
        }
    }

    private void lockedHandleJoinRequest(ChordID newNode, String uri) {
        try {
            if (Debug.isEnabled("chord.debugThreadTiming")) InstrumentedThread.startInvocation("lockedHandleJoinRequest");
            Debug.println("Called locked handle join request for " + newNode + " for " + uri, "chord.JoinDebug");
            byte[] message = null;
            int neighborJoining = -1;
            boolean retry = false;
            ChordNodeInfo requestingNode = new ChordNodeInfo(newNode, uri);
            ChordNodeInfo destination = null;
            ChordNodeInfo nextHop = fingerTable.closestPrecedingFinger(newNode);
            if (nextHop == null) {
                if (!requestingNode.equals(fingerTable.predecessor())) {
                    ChordID newID = splitDomain();
                    if (newID != null) {
                        Debug.println("Locked: Handling duplicate join ID (predecessor) " + newNode + " to " + newID, "chord.JoinDebug");
                        nextHop = fingerTable.predecessor();
                        requestingNode = new ChordNodeInfo(newID, uri);
                        newNode = newID;
                    }
                    else {
                        nextHop = fingerTable.successor();
                        newNode = localID;
                    }
                }
                else {
                    nextHop = fingerTable.predecessor();
                    retry = true;
                }
            }
            if (nextHop.nodeID.equals(fingerTable.predecessor().nodeID) /*&& !realPredecessor*/) {
                // This is the successor of the joining id, so must handle join
                neighborJoining = PREDECESSOR;
                if (!newNode.equals(localID)) {
                    // Reply with finger table to initialize new node
                    Debug.println("Locked: Joining node: " + newNode, "chord.JoinDebug");
                    message = ChordMessage.newJoinAcceptMessage(fingerTable, newNode).serialize();
                    destination = requestingNode;
                    fingerTable.update(requestingNode);
                    joinCache.put(uri, newNode);
                }
                else {
                    if (!uri.equals(localURI.toString())) {
                        // Handle duplicate ID
                        ChordID newID = splitDomain();
                        if (newID != null) {
                            Debug.println("Locked: Handling duplicate join ID (localnode)" + newNode + " to " + newID, "chord.JoinDebug");
                            message = ChordMessage.newJoinAcceptMessage(fingerTable, newID).serialize();
                            requestingNode = new ChordNodeInfo(newID, uri);
                            destination = requestingNode;
                            fingerTable.update(requestingNode);
                            joinCache.put(uri, newID);
                        }
                        else {
                            message = ChordMessage.newJoinMessage(localID, uri, false).serialize();
                            destination = fingerTable.successor();
                            neighborJoining = -1;
                        }
                    }
                    else {
                        destination = null;
                    }
                }
            }
            else {
                // This is not the successor, so pass the join request on, unless joining using broadcast
                // Get closest preceding finger from finger table and send request on
                message = ChordMessage.newJoinMessage(newNode, uri, false).serialize();
                if (!nextHop.getChordID().equals(localID)) {
                    destination = nextHop;
                }
                else {
                    destination = fingerTable.successor();
                    neighborJoining = SUCCESSOR;
                }
            }

            if (destination != null) {
                robustSend(message, destination, (neighborJoining != PREDECESSOR));    // Enable forwarding unless sending a response to the joining predecessor
            }
//                if (requestingNode != null) {     // TODO: See if inclusive updating can be recovered
//                    fingerTable.update(requestingNode);
//                }
            if (neighborJoining >= 0) {
                this.notifyJoin(neighborJoining);
            }
            Debug.println("Locked: Join exit for " + requestingNode.nodeID + " at " + uri, "chord.JoinDebug");
            if (Debug.isEnabled("chord.debugThreadTiming")) InstrumentedThread.endInvocation("lockedHandleJoinRequest");
        }
        catch (Exception e) {
            Debug.printStackTrace(e, "chord");
        }
    }

    /**
     * Called when a join reply is received from a bootstrap node
     */
    protected void completeJoin(String initializerS, ChordID acceptedID) {
        if (!joinReplyReceived.isTrue()) {
            joinReplyReceived.evaluateCondition(true);
            localID = acceptedID;
            fingerTable = new FingerTable(localID, localURI.toString(), REDUNDANT_SUCC_LENGTH);
            fingerTable.initialize(initializerS);
            System.out.println("Initialized");
            if (Debug.isEnabled("chord.FingerTable")) fingerTable.print();
            // Notify predecessor
            System.out.println("Notifying predecessor with " + localID + " and " + localURI);
            ChordMessage updateMessage = ChordMessage.newNodeUpdateMessage(new ChordNodeInfo(localID, localURI.toString()), null);
            try {
                network.send(updateMessage.serialize(), fingerTable.predecessor().nodeURI);
            }
            catch (NetworkException ne) {
                Debug.printStackTrace(ne, "chord");
            }  // Optimization message; does not matter if not sent
        }
        joined.evaluateCondition(true);
    }

    /**
     * Finds the next hop for the resolve request
     */
    protected void relayResolveRequest(ChordID resolveID, String returnTo, boolean failsafeMode) {
        Debug.println("Relaying resolve request for " + resolveID + " from " + returnTo, "chord.ResolveDebug");
        try {
            if (Debug.isEnabled("chord.debugThreadTiming")) InstrumentedThread.startInvocation("relayResolveRequest");
            joined.awaitUninterruptibly();
            ChordMessage message = null;
            ChordNodeInfo returnNode = new ChordNodeInfo(null, returnTo);
            ChordNodeInfo destination = null;
            ChordNodeInfo nextHop = null;
            boolean relaying = false;
            if (resolveID.isLocal()) {  // Must resolve on local ring
                nextHop = fingerTable.closestPrecedingFinger(resolveID);
                if (nextHop != null) {
                    if (!nextHop.nodeID.equals(localID)) {
                        if (!nextHop.getChordID().equals(fingerTable.predecessor().getChordID())) {
                            // Cannot resolve with local finger table; must relay request
                            message = ChordMessage.newResolveRequest(resolveID, returnTo, failsafeMode);
                            destination = nextHop;
                            relaying = true;
                        }
                        else {
                            // Solution is the local node
                            // Is of all clusters relay went here...
                            if (!leaving) {
                                message = ChordMessage.newResolveReply(resolveID, new ChordNodeInfo(localID, localURI.toString()));
                            }
                            else {
                                message = ChordMessage.newResolveReply(resolveID, fingerTable.successor());
                            }
                            destination = returnNode;
                        }
                    }
                    else {  // Solution is the successor
                        // Is of all clusters relay went here...
                        if (!fingerTable.successor().nodeID.equals(localID)) {  // Check to see if the local node is the only one in the network
                            message = ChordMessage.newResolveReply(resolveID, fingerTable.successor());
                            destination = returnNode;
                        }
                        else {
                            if (!leaving) {
                                message = ChordMessage.newResolveReply(resolveID, new ChordNodeInfo(localID, localURI.toString()));
                                destination = returnNode;
                            }
                        }
                    }
                }
                else {  // Solution is the predecessor
                    // Is of all clusters relay went here...
                    message = ChordMessage.newResolveReply(resolveID, fingerTable.predecessor());
                    destination = returnNode;
                }
                if (!relaying && resolveID.isOfAllClusters()) {     // Relay to other rings to resolve there if necessary
                    ChordID remoteResolve = new ChordID(resolveID.value, localID.NUM_BITS); // Create new ID to be resolved (locally) at remote clusters
                    message = ChordMessage.newResolveRequest(remoteResolve, returnTo, failsafeMode);
                    for (ChordNodeInfo successor : remoteSuccessors.values()) {
                        robustSend(message.serialize(), successor, false);
                    }
                }
            }
            else {  // Query cannot be resolved in local ring; must relay to remote successor
                ChordNodeInfo remoteNode = (ChordNodeInfo) remoteSuccessors.get(resolveID.getCluster());
                if (remoteNode != null) {
                    message = ChordMessage.newResolveRequest(resolveID, returnTo, failsafeMode);
                    destination = remoteNode;
                }
            }
            // Send appropriate message created above, or apply locally if solution is local node
            if (destination != null) {
                if (!destination.nodeURI.equals(localURI.toString())) {
//                    if (failsafeMode) { // Check destination before relaying if in failsafe mode
//                        pingReturned = new ConditionVariable();
//                        while (!pingReturned.isTrue()) {
//                            network.send(ChordMessage.newPingMessage(localURI.toString()).serialize(), destination);
//                            if (!pingReturned.await(RETRY_PERIOD, TimeUnit.MILLISECONDS)) {
//                                // Destination didn't respond, fix finger link
//                                timer.schedule(new FixFingersTask(nextHop.getChordID()), FIX_FINGERS_PERIOD);
//                                // Find closestPrecedingFinger to nextHop, and update destination
//                                ChordID searchID = nextHop.getChordID().add(new BigInteger("-1"));
//                                nextHop = fingerTable.closestPrecedingFinger(searchID);
//                                destination = nextHop.nodeURI;
//                            }
//                        }
//                        pingReturned = null;
//                    }
                    if (Debug.isEnabled("chord.ResolveDebug")) {
                        if (message.TYPE == 5) {
                            System.out.println("Sending request on to " + destination.nodeURI);
                        }
                        else if (message.TYPE == -5) {
                            System.out.println("Sending solution to " + destination.nodeURI);
                        }
                    }
                    byte[] messageBytes = message.serialize();
                    robustSend(messageBytes, destination, relaying);
                }
                else {
                    message.applyTo(this);
                }
            }
            if (Debug.isEnabled("chord.debugThreadTiming")) InstrumentedThread.endInvocation("relayResolveRequest");
        }
        catch (Exception e) {
            Debug.printStackTrace(e, "chord");
        }
    }

    // Called when a resolve reply message is received
    protected void completeResolve(ChordID resolveID, ChordNodeInfo resolvedNode) {
        synchronized (resolveRequests) {
            Vector<ChordResolveRequest> requests = resolveRequests.get(resolveID);
            if (requests != null) {
                for (ChordResolveRequest request : requests) {
                    request.addResult(resolvedNode);
                }
                requests.clear();
            }
        }
    }

    /**
     * Finds the next node to deliver the message, using the resolve method on the minimum ID of the destination cluster. If in a
     * two-level overlay and ANY_CLUSTER query, the routing message must keep track of the clusters visited
     */
    protected void relayRouteRequest(ChordIDCluster to, ChordNodeInfo from, String tag, byte[] payload, boolean error, Vector clustersVisited) {
        if (Debug.isEnabled("chord.debugThreadTiming")) InstrumentedThread.startInvocation("relayRouteRequest");
        joined.awaitUninterruptibly();
        ChordIDCluster localCluster = null;
        boolean doNotify = false;
        if (to.elementAt(0).isLocal()) {
            try {
                localCluster = to.consume(fingerTable.predecessor().getChordID(), localID);
                if (!localCluster.isEmpty()) {
                    doNotify = true;
                }
            }
            catch (NullPointerException npe) {
                Debug.println("Error at node " + localID, "chord.RouteDebug");
                Debug.println("Incoming: " + new String(payload), "chord.RouteDebug");
            }
        }
        else {
            to.sort();
        }
        if (!to.isEmpty()) {
            NodeInfo[] destinations = null;
            try {
                destinations = this.resolve(to.elementAt(0));
                Debug.println("Routing " + to.elementAt(0) + " to " + destinations[0], "chord.RouteDebug");
                for (NodeInfo node : destinations) {
                    byte[] message = ChordMessage.newRouteToMessage(to, from, tag, payload, error, clustersVisited).serialize();
                    robustSend(message, (ChordNodeInfo) node, true);
                }
            }
            catch (ResolveException re) {
                Debug.printStackTrace(re, "chord");
            }
        }
        if (doNotify) {
            notifyMessage(localCluster, from, tag, payload, error, clustersVisited);
        }
        if (Debug.isEnabled("chord.debugThreadTiming")) InstrumentedThread.endInvocation("relayRouteRequest");
    }

    // Called when a neighbor leaves the overlay normally
    protected void processDeparture(ChordID nodeID, ChordNodeInfo replacement) {
        joined.awaitUninterruptibly();
        fingerTable.deleteNode(nodeID);
        fingerTable.update(replacement);
    }

    protected void processVirtualChildLeave(ChordNodeInfo node, Vector<ChordNodeInfo> newChildren) {
        joined.awaitUninterruptibly();
        fingerTable.deleteNode(node.getChordID());
        virtualChildren.remove(node);
        virtualChildren.addAll(newChildren);
    }

    protected void processVirtualParentLeave(ChordNodeInfo parent, ChordNodeInfo newParent) {
        joined.awaitUninterruptibly();
        fingerTable.deleteNode(parent.getChordID());
        virtualParent = newParent;
        this.virtualDepth--;    // TODO: If virtual depth becomes important, develop mechanism to keep up to date in rest of tree
    }

    /**
     * Used to complete a join in a two-level overlay; obtains the remote successors of the joining node
     */
    protected void virtualJoin() throws IOException {
        try {
            String remoteBootstrapList = getProperty("chord.REMOTE_BOOTSTRAP_LIST", null);
            if (remoteBootstrapList != null) {
                StringTokenizer listTokenizer = new StringTokenizer(remoteBootstrapList, ",");
                while (listTokenizer.hasMoreElements()) {
                    String nextBootstrapNode = listTokenizer.nextToken();
                    nextBootstrapNode = nextBootstrapNode.trim();
                    ChordID resolveID = new ChordID(localID.value, localID.NUM_BITS);
                    byte[] message = ChordMessage.newResolveRequest(resolveID, localURI.toString(), false).serialize();
                    String destination = nextBootstrapNode;
                    // Relay the resolve request and wait for response
                    int retries = RETRY_LIMIT;
                    ChordResolveRequest request = new ChordResolveRequest(1);
                    Vector<ChordResolveRequest> requests = resolveRequests.get(resolveID);
                    if (requests == null) {
                        requests = new Vector<ChordResolveRequest>();
                        resolveRequests.put(resolveID, requests);
                    }
                    synchronized (requests) {
                        requests.add(request);
                    }
                    while (!request.isDone() && retries > 0) {
                        //Send request
                        network.send(message, destination);
                        // Wait for response (blocking operation)
                        if (!request.waitOn(RETRY_PERIOD, TimeUnit.MILLISECONDS)) {
                            retries--;
                        }
                    }
                    if (request.isDone()) {
                        ChordNodeInfo remoteSuccessor = ((ChordNodeInfo) request.getResult()[0]);
                        String remoteCluster = remoteSuccessor.getChordID().getCluster();
                        if (!remoteCluster.equals(ChordID.LOCAL_CLUSTER)) {
                            remoteSuccessors.put(remoteCluster, remoteSuccessor);
                        }
                        synchronized (requests) {
                            requests.remove(request);
                        }
                    }
                }
                System.out.println("Joined remote successors: " + remoteSuccessors);
            }
        }
        catch (Exception e) {
            throw new IOException("ChordOverlayService: Could not complete virtual join: " + e.getMessage());
        }
    }

    /**
     * Called when a stabilize query is received from the node's predecessor, to keep the successor/predecessor link updated
     */
    protected void processStabilizeRequest(String retUri) {
        try {
            if (Debug.isEnabled("chord.debugThreadTiming")) InstrumentedThread.startInvocation("processStabilizeRequest");
            joined.awaitUninterruptibly();
            ChordNodeInfo reply = fingerTable.predecessor();
            if (reply == null) {
                reply = new ChordNodeInfo(localID, localURI.toString());
            }
            byte[] message = ChordMessage.newStabilizeReply(reply).serialize();
            network.send(message, retUri);
            if (Debug.isEnabled("chord.debugThreadTiming")) InstrumentedThread.endInvocation("processStabilizeRequest");
        }
        catch (Exception e) {
            Debug.printStackTrace(e, "chord");
        }
    }

    /**
     * Called after a reply is received from a stabilize request; used to discover a new or failed successor
     */
    protected void completeStabilize(ChordNodeInfo possiblyNew) {
        boolean doNotifyJoin = false;
        boolean doNotifyFailure = false;
        if (!possiblyNew.getChordID().equals(localID)) {
            // Either this or successor's table is outdated
            try {
                byte[] message = null;
                String destination = null;
                if (failedSuccessor == null) { // Not handling a failure
                    // Notify successor of this node's presence
                    message = ChordMessage.newNotifyMessage(new ChordNodeInfo(localID, localURI.toString())).serialize();
                    if (possiblyNew.getChordID().isBetween(localID, fingerTable.successor().getChordID())) {
                        // New node is between this and current successor, so new node is new successor
                        destination = possiblyNew.nodeURI;
                        doNotifyJoin = true;
                    }
                    else {
                        // This node is between new node and current successor
                        destination = fingerTable.successor().nodeURI;
                    }
                    fingerTable.update(possiblyNew);
                }
                else { // Handling a failure
                    if (possiblyNew.getChordID().equals(failedSuccessor.getChordID())) {
                        // New successor is successor of failed node
                        message = ChordMessage.newRepairMessage(new ChordNodeInfo(localID, localURI.toString())).serialize();
                        destination = fingerTable.successor().nodeURI;
                        failedSuccessor = null;
                        successorKnown.evaluateCondition(true);
                        doNotifyFailure = true;
                    }
                    else {
                        // Haven't found successor of failed node, update list and wait for next try
                        fingerTable.update(possiblyNew);
                    }
                }
                if (destination != null) {
                    network.send(message, destination);
                }
                if (doNotifyJoin) {
                    this.notifyJoin(SUCCESSOR);
                }
                if (doNotifyFailure) {
                    this.notifyFailure(SUCCESSOR, new NodeInfo[] {failedSuccessor}, new NodeInfo[] {possiblyNew});
                }
            }
            catch (Exception e) {
                Debug.printStackTrace(e, "chord");
            }
        }
        stabilizeComplete.evaluateCondition(true);
    }

    /**
     * Called when a node notifies its new successor (which has recently joined) of its presence
     */
    protected void processNotify(ChordNodeInfo possiblePred) {
        joined.awaitUninterruptibly();
        //Will update if necessary
        ChordID currentPredecessorID = fingerTable.predecessor().getChordID();
        fingerTable.update(possiblePred);
        if (!currentPredecessorID.equals(fingerTable.predecessor().getChordID())) {
            // New predecessor has joined, notify listeners
            this.notifyJoin(PREDECESSOR);
        }
    }

    protected void processNodeUpdate(ChordNodeInfo node, ChordID baseID) {
        joined.awaitUninterruptibly();
        fingerTable.update(node);
        ChordMessage updateMessage = null;
        if (baseID != null) {
            BigInteger HALF_RING = baseID.getRingDimension().divide(new BigInteger("2"));
            BigInteger ringDistance = baseID.ringDifference(localID);
            if (ringDistance.compareTo(HALF_RING) < 0 && !ringDistance.equals(BigInteger.ZERO)) {
                updateMessage = ChordMessage.newNodeUpdateMessage(node, baseID);
            }
        }
        else {
            updateMessage = ChordMessage.newNodeUpdateMessage(node, localID);
        }
        if (updateMessage != null) {
            try {
                network.send(updateMessage.serialize(), fingerTable.predecessor().nodeURI);
            }
            catch (NetworkException ne) {
                Debug.printStackTrace(ne, "chord");
            }  // Optimization message; does not matter if not sent
        }
    }

    /**
     * Called after a failure has been detected
     */
    protected void processRepair(ChordNodeInfo forcedPred) {
        //Will update if necessary
        ChordNodeInfo failedNode = fingerTable.predecessor();
        fingerTable.deleteNode(failedNode.getChordID());
        fingerTable.update(forcedPred);
        this.notifyFailure(PREDECESSOR, new NodeInfo[] {failedNode}, new NodeInfo[] {forcedPred});
    }

    /**
     * Sends reply to a ping received from a remote node
     */
    protected void processPing(String replyTo) {
        try {
            network.send(ChordMessage.newPingReply().serialize(), replyTo);
        }
        catch (Exception e) {
            Debug.printStackTrace(e, "chord");
        }
    }

    /**
     * Called when the reply to a ping is received
     */
    protected void completePing() {
        if (pingReturned != null) {
            pingReturned.evaluateCondition(true);
        }
    }

    protected void processCreateVirtual(ChordID nodeID, ChordNodeInfo parentInfo, int depth) {
        joined.awaitUninterruptibly();
        // Check if repeated request
        ChordOverlayService hostedService = hostedVirtualNodes.get(nodeID);
        if (hostedService != null) {
            // Repeated, reacknowledge
            try {
                network.send(ChordMessage.newAckVirtualNodeHost((ChordNodeInfo) hostedService.getLocalID()).serialize(), parentInfo.nodeURI);
            }
            catch (NetworkException ne) {
                Debug.printStackTrace(ne, "chord");
            }
        }
        else {  // New request to this node
            if (this.canHost(nodeID)) {   // Host if possible
                Hashtable virtualProperties = new Hashtable<String, String>();
                virtualProperties.put("chord.LOCAL_ID", nodeID.value.toString());
                virtualProperties.put("chord.LOCAL_CLUSTER", nodeID.getCluster());
                virtualProperties.put("chord.BOOTSTRAP_ADDR", parentInfo.nodeURI);
                ChordOverlayService newVirtual = new ChordOverlayService(virtualProperties);
                newVirtual.setVirtualDepth(depth);
                newVirtual.setVirtualParent(parentInfo);
                try {
                    newVirtual.setHostInstance(this);
                    ChordID newVirtualID = newVirtual.join("//" + localURI.getHost());
                    hostedVirtualNodes.put(nodeID, newVirtual);
                    network.send(ChordMessage.newAckVirtualNodeHost((ChordNodeInfo) newVirtual.getLocalID()).serialize(), parentInfo.nodeURI);
                    for (Entry<String, OverlayMessageListener> listenerEntry : messageSubscribers.entrySet()) {
                        OverlayMessageListener listener = listenerEntry.getValue();
                        OverlayMessageListener newListener = listener.createNew(newVirtual);
                        if (newListener != null) {
                            String listenerTag = listenerEntry.getKey();
                            if (newListener instanceof ReplicationAwareListener) {
                                if (replication.handlingEventsFor(listenerTag)) {
                                    newVirtual.subscribeToMessages((ReplicationAwareListener) newListener,
                                                                    listenerTag,
                                                                    replication.getLocalStateObject(listenerTag).createNew()
                                    );
                                }
                                else {
                                    newVirtual.subscribe((ReplicationAwareListener) newListener,
                                                          listenerTag,
                                                          replication.getLocalStateObject(listenerTag).createNew()
                                    );
                                }
                            }
                            else {
                                newVirtual.subscribeToMessages(newListener, listenerTag);
                            }
                            if (newListener instanceof OverlayStructureListener) {
                                newVirtual.subscribeToEvents((OverlayStructureListener) newListener);
                            }
                        }
                    }
                }
                catch (Exception e) {
                    Debug.printStackTrace(e, "chord");
                }
            }
            else {  // Send on to possible host
                ChordNodeInfo destination = selectPossibleHost(nodeID);
                robustSend(ChordMessage.newCreateVirtualNodeMessage(nodeID, parentInfo, depth).serialize(), destination, false);
                // TODO: May need to send message back to parent saying the request is being forwarded to reset timer
            }
        }
    }

    protected void completeCreateVirtual(ChordNodeInfo childInfo) {
        virtualChildren.add(childInfo);
    }

    protected void setVirtualDepth(int value) {
        virtualDepth = value;
    }

    protected void setVirtualParent(ChordNodeInfo nodeInfo) {
        virtualParent = nodeInfo;
    }

    protected void setHostInstance(ChordOverlayService chordInstance) {
        hostInstance = chordInstance;
    }

    // Start auxiliary classes

    // Sends requests periodically to successor to return their current predecessor
    protected class StabilizeTask extends TimerTask {

        public void run() {
            try {
                if (joined.isTrue() && fingerTable.successor() != null) {
                    byte[] message = ChordMessage.newStabilizeRequest(localURI.toString()).serialize();
                    String destination = fingerTable.successor().nodeURI;
                    int stabilizeTries = RETRY_LIMIT;
                    stabilizeComplete.reset();
                    while (!stabilizeComplete.isTrue() && stabilizeTries > 0) {
                        try {
                            network.send(message, destination);
                        }
                        catch (NetworkException ne) {
                            Debug.println("ChordOverlayService: Cannot reach successor: " + destination, "chord.StabilizeDebug");
                        }
                        stabilizeComplete.awaitUninterruptibly(RETRY_PERIOD, TimeUnit.MILLISECONDS);
                        stabilizeTries--;
                    }
                    if (!stabilizeComplete.isTrue()) {
                        if (successorKnown.isTrue()) {
                            if (fingerTable.getRingState() == FingerTable.RingState.CHORD) {
                                successorKnown.reset();
                            }
//                            notifyFailure(NodeNeighborhood.SUCCESSOR, new NodeInfo[] {failedSuccessor}, );
                        }
                        // Remove current successor from finger table
                        // TODO: Handle multiple simultaneous failures by keeping track of more than one failed successor
                        if (failedSuccessor == null) {
                            failedSuccessor = fingerTable.successor();
                        }
                        fingerTable.deleteNode(fingerTable.successor().getChordID());
                        stabilizeComplete.evaluateCondition(true);
                    }
                }
            }
            catch (Exception e) {
                Debug.printStackTrace(e, "chord");
            }
        }

    }

    // Sends requests periodically to random fingers to update their content, or to a specific finger that is believed to have failed
    protected class FixFingersTask extends TimerTask {

        ChordID fingerToFix = null;

        public FixFingersTask() {}

        public FixFingersTask(ChordID fingerToFix) {
            this.fingerToFix = fingerToFix;
        }

        public void run() {
            try {
                int finger = (fingerToFix == null) ? randomGenerator.nextInt(localID.NUM_BITS - 1) + 1 :
                                                    fingerTable.deleteNode(fingerToFix);
                if (finger <= 0) {
                    finger = randomGenerator.nextInt(localID.NUM_BITS - 1) + 1;
                }
                ChordNodeInfo possibleNewFinger = (ChordNodeInfo) resolve(localID.fingerStep(finger))[0];
                if (possibleNewFinger != null) {
                    fingerTable.update(possibleNewFinger);
//                    Debug.println("After fix", "chord.FixFingersDebug");
//					if (Debug.isEnabled("chord.FingerTable")) fingerTable.print();
                }
                else {
                    Debug.println("Failed to update fingers", "chord.FixFingersDebug");
                }
            }
            catch (Exception e) {
                Debug.printStackTrace(e, "chord");
            }
        }

    }

    // Start private utility methods

    private void notifyDeparture() {
        for (OverlayStructureListener listener : eventSubscribers) {
            listener.leaving();
        }
    }

    private void notifyTerminate() {
        for (OverlayStructureListener listener : eventSubscribers) {
            listener.terminating();
        }
    }

    private void notifyMessage(ChordIDCluster to, ChordNodeInfo from, String tag, byte[] message, boolean error, Vector clustersVisited) {
        OverlayMessageListener listener = messageSubscribers.get(tag);
        ChordMessageCallback callback = new ChordMessageCallback(to, from, tag, message, clustersVisited, this);
        if (listener != null) {
            if (!error) {
                listener.messageArrived(callback);
            }
            else {
                listener.serviceErrorOcurred(callback.getMessageOrigin(), callback.getMessageBytes());
            }
        }
    }

    private void notifyJoin(int neighborType) { //ChordID nodeID, String nodeURI) {
        ChordNodeInfo newNeighbor = ((ChordNodeNeighborhood) this.getNeighborhood()).getLocal(neighborType);
        for (final OverlayStructureListener listener : eventSubscribers) {
            listener.newNeighbor(neighborType, newNeighbor);
        }
    }

    private void notifyFailure(int neighborType, NodeInfo[] failedNodes, NodeInfo[] replacements) { //ChordID nodeID) {
        for (final OverlayStructureListener listener : eventSubscribers) {
            listener.neighborsDown(neighborType, failedNodes, replacements);
        }
    }

    private void robustSend(byte[] message, ChordNodeInfo destination, boolean forward) {
        try {
            Debug.println("Sending message on network to " + destination.nodeURI, "chord.SendDebug");
            if (Debug.isEnabled("chord.debugThreadTiming")) InstrumentedThread.startInvocation("networkSend");
            network.send(message, destination.nodeURI);
            if (Debug.isEnabled("chord.debugThreadTiming")) InstrumentedThread.endInvocation("networkSend");
        }
        catch (NetworkException ne) {
            if (forward) {
                if (Debug.isEnabled("chord.debugThreadTiming")) InstrumentedThread.startInvocation("robustSendForward");
                Debug.println("ChordOverlayService: Unable to send to " + destination.nodeURI, "chord.SendDebug");
                Debug.println("ChordOverlayService: Finding path to send", "chord.SendDebug");
                final byte[] refMessage = message;
                final ChordNodeInfo refDestination = destination;
                final FingerTable refFingerTable = fingerTable;
                final MessagingClient refNetwork = network;
//                new Thread(new Runnable() {
//                    public void run() {
                        boolean sent = false;
                        ChordNodeInfo auxDestination = refDestination;
                        while (!sent) {
                            ChordNodeInfo newDestination = refFingerTable.closestStrictlyPrecedingFinger(auxDestination.getChordID());
                            if (newDestination.getChordID().equals(localID)) {  // Destination is successor, wait until stabilize resolves
                                successorKnown.reset();
                                successorKnown.awaitUninterruptibly(RETRY_PERIOD, TimeUnit.MILLISECONDS);
                                newDestination = fingerTable.successor();
                            }
//                            else {  // Destination is a finger, run fix fingers task
//                                new FixFingersTask(auxDestination.getChordID()).run();
//                            }
                            auxDestination = newDestination;
                            try {
                                refNetwork.send(refMessage, auxDestination.nodeURI);
                                sent = true;
                                if (!successorKnown.isTrue() && auxDestination.getChordID().equals(fingerTable.successor().getChordID())) {
                                    successorKnown.evaluateCondition(true);
                                }
                            }
                            catch (NetworkException ne2) {
                                Debug.println("ChordOverlayService: Finding path to send", "chord.SendDebug");
                            }
                        }
//                    }
//                }).start();
                if (Debug.isEnabled("chord.debugThreadTiming")) InstrumentedThread.endInvocation("robustSendForward");
            }
            else {
                System.err.println("ChordOverlayService: Send error: " + ne.getMessage());
            }
        }
    }

    private String fixURI(String uri) {
        int start = uri.indexOf("//");
        if (start >= 0) {
            start += 2;
        }
        else {
            uri = "//" + uri;
            start = 2;
        }
        int end = uri.indexOf("/", start);
        end = end>=0 ? end : uri.length();
        int divider = uri.indexOf(":", start);
        divider = divider>=0 ? divider : end;
        try {
            String ipString = uri.substring(start, divider);
            if (ipString.equals("localhost") || ipString.equals("127.0.0.1") || ipString.equals("")) {
                String ipResolve = InetAddress.getLocalHost().getHostAddress();
                if (!ipResolve.equals(ipString)) {
                    uri = uri.substring(0, start) + ipResolve + (divider<uri.length() ? uri.substring(divider, uri.length()) : "");
                }
            }
        }
        catch (UnknownHostException uhe) {
            Debug.printStackTrace(uhe, "chord");
        }
        return uri;
    }

    private MessagingClient createNetworkInstance() throws NetworkException {
        MessagingClient ret = null;
        String networkClass = getProperty("chord.NETWORK_CLASS", "TCP");
        if (networkClass.equals("TCP")) {
            ret = new TCPAdapter(localURI.getPort(), LOG_NETWORK);    // If no port is specified will use any local port
        }
        else if (networkClass.equals("UDP")) {
            ret = new UDPClient(localURI.getPort());    // If no port is specified will use any local port
        }
        else if (networkClass.equals("RUDP")) {
            ret = new ReliableUDPClient(localURI.getPort());
        }
		else if(networkClass.equals("SSL")){
            ret = new SSLAdapter(localURI.getPort(), LOG_NETWORK);    // If no port is specified will use any local port
		}

        boolean useSSHTunnel = Boolean.parseBoolean(getProperty("chord.SSH_TUNNELING", "FALSE"));
        if (useSSHTunnel) {
            String privateKey = getProperty("chord.SSH_PRIVATE_KEY", null);
            SSHClientAdapter sshTunnel;
            if (privateKey != null) {
                sshTunnel = new SSHClientAdapter(ret, privateKey);
            }
            else {
                sshTunnel = new SSHClientAdapter(ret);
            }
            String[] hostDescriptors = getProperty("chord.SSH_HOSTS", "").split(";");   // SSH_HOSTS should contain a semicolon separated list of host descriptors. Each descriptor is [host,port,username,keyfile] (if the keyfile is ommitted, it is assumed that one saved with the same hostname is in the working directory
            for (String hostDescriptor : hostDescriptors) {
                Debug.println("For descriptor: " + hostDescriptor);
                if (hostDescriptor.matches("\\[.+\\]")) {
                    hostDescriptor = hostDescriptor.substring(1, hostDescriptor.length()-1);
                }
                String[] hostInfo = hostDescriptor.split(",");
                if (hostInfo.length >= 3) {
                    try {
                        String host = hostInfo[0];
                        Debug.println(host);
                        int port = Integer.parseInt(hostInfo[1]);
                        Debug.println(port);
                        String userName = hostInfo[2];
                        Debug.println(userName);
                        if (privateKey == null) {
                            sshTunnel.addHost(host, port, userName, hostInfo[3]/*Key file*/);
                        }
                        else {
                            Debug.println("Adding host with default key");
                            sshTunnel.addHost(host, port, userName);
                            Debug.println("Done adding host");
                        }
                    }
                    catch (Exception e) {
                        Debug.printStackTrace(e);
                        throw new IllegalArgumentException("ChordOverlayService: Cannot create network instance: Invalid SSH host descriptor", e);
                    }
                }
                else {
                    throw new IllegalArgumentException("ChordOverlayService: Cannot create network instance: Invalid SSH host descriptor: " + hostDescriptor);
                }
            }
            ret = sshTunnel;
        }
        String resolveLocalString = System.getProperty("chord.NAT_TABLE", null);
        if (resolveLocalString != null) {
            NATAdapter localAdapter = new NATAdapter(ret);
            String[] resolveLocalPairs = resolveLocalString.split(";");
            for (String pairString : resolveLocalPairs) {
                if (pairString.matches("\\[.+\\]")) {
                    pairString = pairString.substring(1, pairString.length()-1);
                }
                String[] pair = pairString.split(",");
                localAdapter.addAddressTranslation(pair[0]/*fromAddress*/, pair[1]/*toAddress*/);
            }
            ret = localAdapter;
        }
        return ret;
    }

    // Returns the ID between the local ID and the predecessor ID, or null if the difference between the two
    // is less than two.
    private ChordID splitDomain() {
        ChordID ret = null;
        BigInteger TWO = new BigInteger("2");
        if (fingerTable.predecessor() != null) {
            if (!fingerTable.predecessor().nodeID.equals(localID)) {
                BigInteger diff = localID.ringDifference(fingerTable.predecessor().getChordID());
                if (diff.compareTo(TWO) >= 0) {
                    ret = fingerTable.predecessor().getChordID().add(diff.divide(TWO));
                }
            }
            else {
                ret = localID.add(localID.RING_DIMENSION.divide(TWO));
            }
        }
        else {
            ret = localID.add(localID.RING_DIMENSION.divide(TWO));
        }
        return ret;
    }

    private String getProperty(String propertyName, String defaultValue) {
        String ret = null;
        if (propertyTable != null) {
            ret = propertyTable.get(propertyName);
        }
        if (ret == null) {
            ret = System.getProperty(propertyName, defaultValue);
        }
        return ret;
    }

    private boolean canHost(ChordID nodeID) {
        boolean ret = true;
        try {
            ChordID nodeSuccessor = ((ChordNodeInfo) this.resolve(nodeID)[0]).getChordID();
            if (hostedVirtualNodes.containsKey(nodeSuccessor) || nodeSuccessor.equals(localID)) {
                ret = false;
            }
            else {
                for (ChordID hostedNode : hostedVirtualNodes.keySet()) {
                    ChordID hostedNodeSuccessor = ((ChordNodeInfo) this.resolve(hostedNode)[0]).getChordID();
                    if (hostedNodeSuccessor.equals(nodeSuccessor)) {
                        ret = false;
                    }
                }
            }
        }
        catch (ResolveException ioe) {
        }
        if (ret && loadManager != null) {
            ret = loadManager.allowNewNode(nodeID);
        }
        return ret;
    }

    private ChordNodeInfo selectPossibleHost(ChordID nodeID) {
        ChordNodeInfo ret = null;    // Select local node by default
        if (loadManager != null) {  // Consult load manager if one has been attached
            ret = (ChordNodeInfo) loadManager.selectHostNode(nodeID);
        }
        else {
            try {
                ChordID hashID = new ChordID(localID.NUM_BITS);
                ret = (ChordNodeInfo) this.resolve(hashID)[0];
            }
            catch (ResolveException ioe) {
                ret = fingerTable.successor();
            }
        }
        return ret;
    }

    private void waitUntilJoined(String methodName) {
        try {
            joined.await(RETRY_PERIOD, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException ie) {}
        if (!joined.isTrue()) {
            throw new IllegalStateException("ChordOverlayService: " + methodName + ": Not joined");
        }
    }

}
