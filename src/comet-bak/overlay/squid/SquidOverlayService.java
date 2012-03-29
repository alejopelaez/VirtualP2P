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

/*
 * SquidOverlayService.java
 *
 * Created on August 23, 2007, 11:39 AM
 */

package tassl.automate.overlay.squid;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.util.Hashtable;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import programming5.arrays.ArrayOperations;
import programming5.collections.RotatingVector;
import programming5.io.Debug;
import programming5.io.InstrumentedThread;
import tassl.automate.overlay.MessageCallback;
import tassl.automate.overlay.NodeInfo;
import tassl.automate.overlay.NodeNeighborhood;
import tassl.automate.overlay.OverlayListener;
import tassl.automate.overlay.OverlayMessageListener;
import tassl.automate.overlay.OverlayService;
import tassl.automate.overlay.OverlayStructureListener;
import tassl.automate.overlay.chord.ChordID;
import tassl.automate.overlay.chord.ChordNodeInfo;
import tassl.automate.overlay.chord.ChordOverlayService;
import tassl.automate.overlay.ApplicationState;
import tassl.automate.overlay.ResolveException;
import tassl.automate.overlay.replication.ReplicationAwareListener;
import tassl.automate.overlay.replication.ReplicationLayer;
import tassl.automate.overlay.management.LoadManager;

/**
 *
 * @author aquirozh
 */
public class SquidOverlayService implements OverlayService<SquidKey>, OverlayMessageListener {

    // Load properties file
    static {
        Properties p = new Properties(System.getProperties());
        try {
            p.load(new FileInputStream("squid.properties"));
            System.setProperties(p);
        }
        catch (FileNotFoundException fnf) {
            System.out.println("No Squid properties file");
        }
        catch (IOException ioe) {
            System.out.println("Bad Squid properties file");
        }
    }

    // Squid objects
    protected ChordOverlayService chord = null;
    protected ChordNodeInfo localNode;
    protected SquidKey myID;
    protected HilbertSFC mapping;
    protected ReplicationLayer replication;

    // Configuration fields and properties
    protected final int RETRY_LIMIT;
    protected final long RETRY_PERIOD;

    // Utility objects
    protected Hashtable<String, OverlayMessageListener> messageSubscribers = new Hashtable<String, OverlayMessageListener>();
    protected Hashtable<String, String> propertyTable;
    private Vector<BigInteger> messageHashStore = new RotatingVector<BigInteger>(20);
    private final Hashtable<Integer, Hashtable<BigInteger, SquidResolveRequest>> resolveRequests = new Hashtable<Integer, Hashtable<BigInteger, SquidResolveRequest>>();
    private final Random random;

    // State variables
    boolean isLeaving = false;

    // Load balancing by hashing
    protected final int BIT_LENGTH; //@anirudh
    public static String HASH_TYPE;//@anirudh - deteriming type of hashing.
    public static int NUM_TASKS;//@anirudh - total number of tasks from squid.properties
    static int counter; //@anirudh

    public SquidOverlayService() {
        propertyTable = null;
        RETRY_LIMIT = Integer.parseInt(getProperty("squid.RETRY_LIMIT", "3"));
        RETRY_PERIOD = Long.parseLong(getProperty("squid.RETRY_PERIOD", "5000"));

        // Load balancing by hashing @anirudh
        BIT_LENGTH = Integer.parseInt(getProperty("squid.BIT_LENGTH", "16"));
        HASH_TYPE = getProperty("squid.HASH_TYPE","None");
        NUM_TASKS = Integer.parseInt(getProperty("squid.NUM_TASKS","100"));
        Debug.println("Hashing type: "+HASH_TYPE, "LoadBalance");

        random = new Random(System.currentTimeMillis() * this.toString().hashCode());
        chord = new ChordOverlayService();
        String debugSets = getProperty("squid.DEBUG", null);
        if (debugSets != null) {
            if (!debugSets.isEmpty()) {
                String[] setNames = debugSets.split(",");
                for (String setName : setNames) {
                    int prefix = setName.indexOf(".");
                    if (prefix >= 0) {
                        Debug.enable(setName);
                    }
                    else {
                        Debug.enable("squid." + setName);
                    }
                }
            }
            else {
                Debug.enable("squid");
            }
        }
    }

    public SquidOverlayService(Hashtable<String, String> myPropertyTable) {
        propertyTable = myPropertyTable;
        RETRY_LIMIT = Integer.parseInt(getProperty("squid.RETRY_LIMIT", "3"));
        RETRY_PERIOD = Long.parseLong(getProperty("squid.RETRY_PERIOD", "5000"));

        // Load balancing by hashing
        BIT_LENGTH = Integer.parseInt(getProperty("squid.BIT_LENGTH", "16"));//@anirudh
        HASH_TYPE = getProperty("squid.HASH_TYPE","None");
        NUM_TASKS = Integer.parseInt(getProperty("squid.NUM_TASKS","100"));
        Debug.println("Hashing type: "+HASH_TYPE, "LoadBalance");

        random = new Random(System.currentTimeMillis() * myPropertyTable.hashCode());
        chord = new ChordOverlayService(myPropertyTable);
        String debugSets = getProperty("squid.DEBUG", null);
        if (debugSets != null) {
            if (!debugSets.isEmpty()) {
                String[] setNames = debugSets.split(",");
                for (String setName : setNames) {
                    int prefix = setName.indexOf(".");
                    if (prefix >= 0) {
                        Debug.enable(setName);
                    }
                    else {
                        Debug.enable("squid." + setName);
                    }
                }
            }
            else {
                Debug.enable("squid");
            }
        }
    }

    public SquidOverlayService(String savedChordNode) throws IOException {
        try {
            propertyTable = null;
            RETRY_LIMIT = Integer.parseInt(getProperty("squid.RETRY_LIMIT", "3"));
            RETRY_PERIOD = Long.parseLong(getProperty("squid.RETRY_PERIOD", "5000"));

            // Load balancing by hashing
            BIT_LENGTH = Integer.parseInt(getProperty("squid.BIT_LENGTH", "16"));//@anirudh
            HASH_TYPE = getProperty("squid.HASH_TYPE","None");
            NUM_TASKS = Integer.parseInt(getProperty("squid.NUM_TASKS","100"));
            Debug.println("Hashing type: "+HASH_TYPE, "LoadBalance");

            chord = new ChordOverlayService(savedChordNode);
            random = new Random(System.currentTimeMillis() * savedChordNode.hashCode());
            String debugSets = getProperty("squid.DEBUG", null);
            if (debugSets != null) {
                if (!debugSets.isEmpty()) {
                    String[] setNames = debugSets.split(",");
                    for (String setName : setNames) {
                        int prefix = setName.indexOf(".");
                        if (prefix >= 0) {
                            Debug.enable(setName);
                        }
                        else {
                            Debug.enable("squid." + setName);
                        }
                    }
                }
                else {
                    Debug.enable("squid");
                }
            }
        }
        catch (IOException ioe) {
            System.out.println("SquidOverlayService: Cannot start with given Chord reference: " + ioe.getMessage());
            throw ioe;
        }
    }

    public SquidOverlayService(byte[] savedChordNode) throws IOException {
        try {
            propertyTable = null;
            RETRY_LIMIT = Integer.parseInt(getProperty("squid.RETRY_LIMIT", "3"));
            RETRY_PERIOD = Long.parseLong(getProperty("squid.RETRY_PERIOD", "5000"));

            // Load balancing by hashing
            BIT_LENGTH = Integer.parseInt(getProperty("squid.BIT_LENGTH", "16"));//@anirudh
            HASH_TYPE = getProperty("squid.HASH_TYPE","None");
            NUM_TASKS = Integer.parseInt(getProperty("squid.NUM_TASKS","100"));
            Debug.println("Hashing type: "+HASH_TYPE, "LoadBalance");

            chord = new ChordOverlayService(savedChordNode);
            random = new Random(System.currentTimeMillis() * savedChordNode.hashCode());
            String debugSets = getProperty("squid.DEBUG", null);
            if (debugSets != null) {
                if (!debugSets.isEmpty()) {
                    String[] setNames = debugSets.split(",");
                    for (String setName : setNames) {
                        int prefix = setName.indexOf(".");
                        if (prefix >= 0) {
                            Debug.enable(setName);
                        }
                        else {
                            Debug.enable("squid." + setName);
                        }
                    }
                }
                else {
                    Debug.enable("squid");
                }
            }
        }
        catch (IOException ioe) {
            System.out.println("SquidOverlayService: Cannot start with given Chord reference: " + ioe.getMessage());
            throw ioe;
        }
    }

    public SquidOverlayService(byte[] savedChordNode, Hashtable<String, String> myProperties) throws IOException {
        try {
            propertyTable = myProperties;
            RETRY_LIMIT = Integer.parseInt(getProperty("squid.RETRY_LIMIT", "3"));
            RETRY_PERIOD = Long.parseLong(getProperty("squid.RETRY_PERIOD", "5000"));

            // Load balancing by hashing
            BIT_LENGTH = Integer.parseInt(getProperty("squid.BIT_LENGTH", "16"));//@anirudh
            HASH_TYPE = getProperty("squid.HASH_TYPE","");
            NUM_TASKS = Integer.parseInt(getProperty("squid.NUM_TASKS","100"));
            Debug.println("Hashing type: "+HASH_TYPE, "LoadBalance");

            chord = new ChordOverlayService(savedChordNode, myProperties);
            random = new Random(System.currentTimeMillis() * savedChordNode.hashCode());
            String debugSets = getProperty("squid.DEBUG", null);
            if (debugSets != null) {
                if (!debugSets.isEmpty()) {
                    String[] setNames = debugSets.split(",");
                    for (String setName : setNames) {
                        int prefix = setName.indexOf(".");
                        if (prefix >= 0) {
                            Debug.enable(setName);
                        }
                        else {
                            Debug.enable("squid." + setName);
                        }
                    }
                }
                else {
                    Debug.enable("squid");
                }
            }
        }
        catch (IOException ioe) {
            System.out.println("SquidOverlayService: Cannot start with given Chord reference: " + ioe.getMessage());
            throw ioe;
        }
    }

    protected SquidOverlayService(ChordOverlayService chordRef) {
        propertyTable = null;
        RETRY_LIMIT = Integer.parseInt(getProperty("squid.RETRY_LIMIT", "3"));
        RETRY_PERIOD = Long.parseLong(getProperty("squid.RETRY_PERIOD", "5000"));

        // Load balancing by hashing
        BIT_LENGTH = Integer.parseInt(getProperty("squid.BIT_LENGTH", "16"));//@anirudh
        HASH_TYPE = getProperty("squid.HASH_TYPE","None");
        NUM_TASKS = Integer.parseInt(getProperty("squid.NUM_TASKS","100"));
        Debug.println("Hashing type: "+HASH_TYPE, "LoadBalance");

        chord = chordRef;
        random = new Random(System.currentTimeMillis() * chordRef.hashCode());
        String debugSets = getProperty("squid.DEBUG", null);
        if (debugSets != null) {
            if (!debugSets.isEmpty()) {
                String[] setNames = debugSets.split(",");
                for (String setName : setNames) {
                    int prefix = setName.indexOf(".");
                    if (prefix >= 0) {
                        Debug.enable(setName);
                    }
                    else {
                        Debug.enable("squid." + setName);
                    }
                }
            }
            else {
                Debug.enable("squid");
            }
        }
    }

    public String saveState() throws IOException {
        return chord.saveState();
    }

    @Override
    public SquidKey join(String uri) throws IOException {
        try {
            String keyspace, nodeURI;
            try {
                URI uriParser = new URI(uri);
                keyspace = getProperty("squid.KEYSPACE", uriParser.getScheme());    // The uri scheme should identify the keyspace
                nodeURI = uriParser.getSchemeSpecificPart();
            }
            catch (Exception use) {
                keyspace = null;
                nodeURI = null;
            }
            if (keyspace != null) {
                myID = this.generateID(keyspace);
            }
            else {
                myID = this.generateID();
            }
            Hashtable<String, String> chordProperties = new Hashtable<String, String>();
            chordProperties.put("chord.ID_BITS", Integer.toString(myID.getSpaceDimensions() * myID.getBitLength()));
            if (propertyTable != null) {
                chordProperties.putAll(propertyTable);
            }
            chord.resetProperties(chordProperties);
            ChordID nodeID = chord.join(nodeURI);
            myID.addToMapping(nodeID);
            localNode = (ChordNodeInfo) chord.getLocalID();
            chord.subscribeToMessages(this, "squid");
            String useMapping = getProperty("squid.USE_MAPPING", null);
            if (useMapping == null) {
                mapping = new HilbertSFC(myID.getSpaceDimensions(), myID.getBitLength());
    //            mapping = new StaticHilbertSFC(myID.getSpaceDimensions(), myID.getBitLength());
    //            mapping.save();
            }
            else {
                if (useMapping.equals("")) {
                    mapping = StaticHilbertSFC.loadFromFile();
                }
                else {
                    mapping = StaticHilbertSFC.loadFromFile(useMapping);
                }
            }
        }
        catch (Exception e) {
            throw new IOException("SquidOverlayService: Cannot complete join: " + e.getMessage());
        }
        return myID;
    }

    @Override
    public void leave() {
        isLeaving = true;
        chord.unsubscribeFromMessages(this, "squid");
        chord.leave();
    }

    public void terminate() {
        isLeaving = true;
        chord.unsubscribeFromMessages(this, "squid");
        chord.terminate();
    }

    @Override
    public SquidKey generateID(Object... parameters) {
        IllegalArgumentException iae = new IllegalArgumentException("SquidOverlayService: Could not generate ID: Parameters: [String | SquidKey]");
        SquidKey ret = null;
        if (parameters.length == 0) {
            String keyspace = getProperty("squid.KEYSPACE", null);
            if (keyspace == null) {
                int spaceDimensions = Integer.parseInt(getProperty("squid.SPACE_DIMENSIONS", "1"));
                SquidKey.KeyTypes[] types = new SquidKey.KeyTypes[spaceDimensions];
                for (int i = 0; i < spaceDimensions; i++) {
                    types[i] = SquidKey.KeyTypes.valueOf(getProperty("squid.D" + Integer.toString(i) + ".KEY_TYPE", "NUMERIC"));
                }
                ret = new SquidKey(spaceDimensions,
                                   Integer.parseInt(getProperty("squid.BIT_LENGTH", "16")),
                                   types
                );
            }
            else {
                int spaceDimensions = Integer.parseInt(getProperty("squid." + keyspace + ".SPACE_DIMENSIONS", "1"));
                SquidKey.KeyTypes[] types = new SquidKey.KeyTypes[spaceDimensions];
                for (int i = 0; i < spaceDimensions; i++) {
                    types[i] = SquidKey.KeyTypes.valueOf(getProperty("squid." + keyspace + ".D" + Integer.toString(i) + ".KEY_TYPE", "NUMERIC"));
                }
                ret = new SquidKey(keyspace,
                                   spaceDimensions,
                                   Integer.parseInt(getProperty("squid." + keyspace + ".BIT_LENGTH", "16")),
                                   types
                );
            }
        }
        else if (parameters.length == 1) {
            if (parameters[0] instanceof String) {
                String keyspace = (String) parameters[0];
                int spaceDimensions = Integer.parseInt(getProperty("squid." + keyspace + ".SPACE_DIMENSIONS", "1"));
                SquidKey.KeyTypes[] types = new SquidKey.KeyTypes[spaceDimensions];
                for (int i = 0; i < spaceDimensions; i++) {
                    types[i] = SquidKey.KeyTypes.valueOf(getProperty("squid." + keyspace + ".D" + Integer.toString(i) + ".KEY_TYPE", "NUMERIC"));
                }
                ret = new SquidKey(keyspace,
                                   spaceDimensions,
                                   Integer.parseInt(getProperty("squid." + keyspace + ".BIT_LENGTH", "16")),
                                   types
                );
            }
            else if (parameters[0] instanceof SquidKey) {
                ret = new SquidKey((SquidKey) parameters[0]);
            }
            else {
                throw iae;
            }
        }
        else {
            throw iae;
        }
        return ret;
    }

    // Fix: User message should contain all info from origin to create callback object at receiver
    @Override
    public void routeTo(SquidKey peers, String tag, byte[] payload) {
        if (isLeaving) throw new IllegalStateException("SquidOverlayService: Cannot route: Leaving overlay");
        if (peers.getKeyspace().equals(myID.getKeyspace())) {
            if (!peers.hasRanges()) {   // Single destination
                BigInteger chordIndex = mapping.coordinatesToIndex(peers.getKeyBits());

                // Load balancing by hashing
                //@ anirudh: Hashing function called here.
                if (HASH_TYPE.equalsIgnoreCase("HASH")){
                    chordIndex = hashIndex(chordIndex);
                }else if (HASH_TYPE.equalsIgnoreCase("BIT_SHIFT")){
                    chordIndex = bitShift(chordIndex);
                }

                peers.addToMapping(chord.generateID(chordIndex));
                Debug.println("Squid: Single routing to " + chordIndex, "squid.RouteDebug");
                chord.routeTo(chord.generateID(chordIndex), "squid", SquidMessage.newUserMessage(peers, localNode, tag, payload).serialize());
            }
            else {  // Multiple destinations
                // Only handles one continuous range for now
                BigInteger[][] indexRanges = peers.getKeyRanges();
                BigInteger[] range = new BigInteger[2*indexRanges.length];
                for (int i = 0; i < indexRanges.length; i++) {
                    range[2*i] = indexRanges[i][0];
                    range[2*i+1] = (indexRanges[i].length > 1) ? indexRanges[i][1] : indexRanges[i][0];
                }
                this.processRouteRequest(range, 0, null, SquidMessage.newUserMessage(peers, localNode, tag, payload));
            }
        }
        else {
            throw new IllegalArgumentException("SquidOverlayService: Cannot route message: Keyspace for SquidKey does not match this squid instance");
        }
    }

    @Override
    public void routeTo(List<SquidKey> peers, String tag, byte[] payload) {
        if (isLeaving) throw new IllegalStateException("SquidOverlayService: Cannot route: Leaving overlay");
        for (SquidKey peer : peers) {
            this.routeTo(peer, tag, payload);
        }
    }

    @Override
    public void reliableRouteTo(SquidKey peers, String tag, byte[] payload) throws IOException {
        System.out.println("SquidOverlayService: Warning: Reliable route to the same as routeTo");
        routeTo(peers, tag, payload);
    }

    @Override
    public void reliableRouteTo(List<SquidKey> peers, String tag, byte[] payload) throws IOException {
        System.out.println("SquidOverlayService: Warning: Reliable route to the same as routeTo");
        routeTo(peers, tag, payload);
    }

    @Override
    public void sendDirect(NodeInfo destination, String tag, byte[] payload) throws IOException {
        if (isLeaving) throw new IllegalStateException("SquidOverlayService: Cannot send: Leaving overlay");
        SquidKey destKey = this.generateID(myID);
        if (destination.nodeID != null) {
            destKey.addToMapping((ChordID) destination.nodeID);
        }
        chord.sendDirect(destination, "squid", SquidMessage.newUserMessage(destKey, localNode, tag, payload).serialize());
    }

//    public void virtualGroupDownstreamSend(String tag, byte[] payload) throws IOException {
//        chord.virtualGroupDownstreamSend("squid", SquidMessage.newUserMessage(localNode, tag, payload).serialize());
//    }
//
//    public void virtualGroupUpstreamSend(String tag, byte[] payload) throws IOException {
//        chord.virtualGroupUpstreamSend("squid", SquidMessage.newUserMessage(localNode, tag, payload).serialize());
//    }

    @Override
    public NodeInfo[] resolve(SquidKey peer) throws ResolveException {
        if (isLeaving) throw new IllegalStateException("SquidOverlayService: Cannot resolve: Leaving overlay");
        NodeInfo[] ret = null;
        if (peer.getKeyspace().equals(myID.getKeyspace())) {
            if (!peer.hasRanges()) {   // Single destination
                BigInteger chordIndex = mapping.coordinatesToIndex(peer.getKeyBits());

                // Load balancing by hashing  //@anirudh
                if (HASH_TYPE.equalsIgnoreCase("HASH")){
                    chordIndex = hashIndex(chordIndex);
                }else if (HASH_TYPE.equalsIgnoreCase("BIT_SHIFT")){
                    chordIndex = bitShift(chordIndex);
                }

                ret = chord.resolve(chord.generateID(chordIndex));
                peer.addToMapping((ChordID) ret[0].nodeID);
            }
            else {  // Multiple destinations
                // Only handles one continuous range for now
                BigInteger[][] indexRanges = peer.getKeyRanges();
                BigInteger[] range = new BigInteger[2*indexRanges.length];
                for (int i = 0; i < indexRanges.length; i++) {
                    range[2*i] = indexRanges[i][0];
                    range[2*i+1] = (indexRanges[i].length > 1) ? indexRanges[i][1] : indexRanges[i][0];
                }
                BigInteger callKey = new BigInteger(64, random);
                this.processResolveRequest(range, 0, null, SquidMessage.newResolveRequest(peer.hashCode(), callKey, localNode));
                SquidResolveRequest processedRequest = resolveRequests.get(peer.hashCode()).get(callKey);
                if (processedRequest.isDone()) {
                    ret = processedRequest.getResult();
                    for (NodeInfo node : ret) {
                        peer.addToMapping((ChordID) node.nodeID);
                    }
                }
                else {
                    throw new ResolveException("SquidOverlayService", "Did not get all replies from network");
                }
            }
        }
        else {
            throw new IllegalArgumentException("SquidOverlayService: Cannot execute resolve: Keyspace for SquidKey does not match this squid instance");
        }
        return ret;
    }

    @Override
    public NodeInfo getLocalID() {
        return localNode;
    }

    @Override
    public NodeInfo[] getNeighborSet() {
        return chord.getNeighborSet();
    }

    @Override
    public NodeNeighborhood getNeighborhood() {
        return chord.getNeighborhood();
    }

    @Override
    public void subscribe(OverlayListener listener, String tag) {
        this.subscribeToEvents(listener);
        this.subscribeToMessages(listener, tag);
    }

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
    public void unsubscribe(OverlayListener listener, String tag) {
        this.unsubscribeFromEvents(listener);
        this.unsubscribeFromMessages(listener, tag);
    }

    @Override
    public void subscribeToMessages(OverlayMessageListener listener, String tag) {
        messageSubscribers.put(tag, listener);
    }

    @Override
    public void subscribeToMessages(ReplicationAwareListener listener, String tag, ApplicationState stateObject) {
        if (replication == null) {
            replication = new ReplicationLayer(this, Integer.parseInt(getProperty("overlay.replication.NUM_REPLICAS", "1")));
            this.subscribeToEvents(replication);
        }
        replication.replicateFor(tag, listener, stateObject, true);
        this.subscribeToMessages(replication, tag);
    }

    @Override
    public void unsubscribeFromMessages(OverlayMessageListener listener, String tag) {
        messageSubscribers.remove(tag);
    }

    @Override
    public void subscribeToEvents(OverlayStructureListener listener) {
        chord.subscribeToEvents(listener);
    }

    @Override
    public void unsubscribeFromEvents(OverlayStructureListener listener) {
        chord.unsubscribeFromEvents(listener);
    }

    @Override
    public void attachLoadManager(LoadManager managerRef) {
        chord.attachLoadManager(managerRef);
    }

    @Override
    public void createVirtualNode(SquidKey nodeID) {
        chord.createVirtualNode(nodeID.getChordMapping().get(0));
    }

    @Override
    public void messageArrived(MessageCallback c) {
        SquidMessage message = (SquidMessage) SquidMessage.createFromBytes(c.getMessageBytes());
        if (message.isUserMsg()) {
            Debug.println("Delivering user message", "squid.RouteDebug");
            deliverMessage(message);
        }
        else {
            message.applyTo(this);
        }
    }

    @Override
    public void serviceErrorOcurred(NodeInfo where, byte[] errorMessage) {
    }

    @Override
    public OverlayMessageListener createNew(OverlayService serviceRef) {
        SquidOverlayService ret = new SquidOverlayService((ChordOverlayService) serviceRef);
        try {
            ret.myID = new SquidKey(myID);
            ret.myID.addToMapping((ChordID) serviceRef.getLocalID().nodeID);
            ret.localNode = (ChordNodeInfo) serviceRef.getLocalID();
            String useMapping = getProperty("squid.USE_MAPPING", null);
            if (useMapping == null) {
                ret.mapping = new HilbertSFC(myID.getSpaceDimensions(), myID.getBitLength());
    //            mapping = new StaticHilbertSFC(myID.getSpaceDimensions(), myID.getBitLength());
    //            mapping.save();
            }
            else {
                if (useMapping.equals("")) {
                    ret.mapping = StaticHilbertSFC.loadFromFile();
                }
                else {
                    ret.mapping = StaticHilbertSFC.loadFromFile(useMapping);
                }
            }
            for (Entry<String, OverlayMessageListener> listenerEntry : messageSubscribers.entrySet()) {
                OverlayMessageListener listener = listenerEntry.getValue();
                OverlayMessageListener newListener = listener.createNew(ret);
                if (newListener != null) {
                    String listenerTag = listenerEntry.getKey();
                    if (newListener instanceof ReplicationAwareListener) {
                        if (replication.handlingEventsFor(listenerTag)) {
                            ret.subscribeToMessages((ReplicationAwareListener) newListener,
                                                    listenerTag,
                                                    replication.getLocalStateObject(listenerTag).createNew()
                            );
                        }
                        else {
                            ret.subscribe((ReplicationAwareListener) newListener,
                                           listenerTag,
                                           replication.getLocalStateObject(listenerTag).createNew()
                            );
                        }
                    }
                    else {
                        ret.subscribeToMessages(newListener, listenerTag);
                        if (newListener instanceof OverlayStructureListener) {
                            ret.subscribeToEvents((OverlayStructureListener) newListener);
                        }
                    }
                }
            }
        }
        catch (Exception e) {
            Debug.printStackTrace(e, "squid");
        }
        return ret;
    }

    public void leaving() {
        isLeaving = true;
    }

    public void terminating() {
        isLeaving = true;
    }

//    public void newNeighbor(NodeInfo newNode) {
////        for (OverlayStructureListener listener : eventSubscribers) {
////            listener.newNeighbor(newNode);
////        }
//    }
//
//    public void newNeighbor(int newNodePosition) {
//        for (OverlayStructureListener listener : eventSubscribers) {
//            listener.newNeighbor(newNodePosition);
//        }
//    }
//
//    public void neighborDown(OverlayID absentNode) {
////        for (OverlayStructureListener listener : eventSubscribers) {
////            listener.neighborDown(absentNode);
////        }
//    }
//
//    public void neighborDown(int absentNodePosition) {
//        for (OverlayStructureListener listener : eventSubscribers) {
//            listener.neighborDown(absentNodePosition);
//        }
//    }

    @Override
    public void updateApplicationState(String tag, ApplicationState stateObject) {
        if (replication != null) {
            replication.replicateState(tag, stateObject);
        }
    }

    /*
    public void simulateNetworkFailure() {
        chord.simulateNetworkFailure();
    }
     */

    public void recoverNetwork() {
        chord.recoverNetwork();
    }

    protected void processRouteRequest(BigInteger[] range, int refinement, BigInteger partialIndex, SquidMessage userMessage) {
        if (Debug.isEnabled("squid.debugThreadTiming")) InstrumentedThread.startInvocation("squidProcessRouteRequest");
        if (!isLeaving) {
            boolean refineFurther = true;
            boolean doDeliver = false;
            if (refinement > 0) {
                if (this.covers(partialIndex, refinement)) {
                    refineFurther = false;
                    doDeliver = true;
                }
                else {
                    BigInteger[] coords = mapping.indexToCoordinates(((ChordID) chord.getLocalID().nodeID).getIndexValue());
                    boolean included = true;
                    for (int i = 0; i < coords.length; i++) {
                        if (coords[i].compareTo(range[2*i]) < 0 || coords[i].compareTo(range[2*i+1]) > 0) {
                            included = false;
                            break;
                        }
                    }
                    if (included) {
                        doDeliver = true;
//                    final SquidMessage auxMessage = userMessage;
//                    new Thread(new Runnable() {
//                        public void run() {
//                            deliverMessage(auxMessage);
//                        }
//                    }).start();
                    }
                }
            }
            if (refineFurther) {
                ClusterRefiner refiner = new ClusterRefiner(mapping);
                refiner.refine(range, refinement+1);
                // TODO: Possibly check if two or more indices can be sent to the same node
                int numDestinations = refiner.getDivisionSize();
                for (int i = 0; i < numDestinations; i++) {
                    byte[] chordMessage = SquidMessage.newRoutingMessage(refiner.getRange(i), refiner.currentRefinement(), refiner.getIndex(i), userMessage).serialize();
                    chord.routeTo(chord.generateID(refiner.getIndex(i)), "squid", chordMessage);
                }
            }
            if (Debug.isEnabled("squid.debugThreadTiming")) InstrumentedThread.endInvocation("squidProcessRouteRequest");
            if (doDeliver) {
                deliverMessage(userMessage);
            }
        }
    }

    protected void processResolveRequest(BigInteger[] range, int refinement, BigInteger partialIndex, SquidMessage resolveMessage) {
        if (Debug.isEnabled("squid.debugThreadTiming")) InstrumentedThread.startInvocation("squidProcessResolveRequest");
        if (Debug.isEnabled("squid.ResolveDebug")) {
            System.out.println("Resolving range: ");
            ArrayOperations.printHorizontal(range);
        }
        if (!isLeaving) {
            boolean refineFurther = true;
            boolean includeLocal = false;
            SquidResolveRequest request = null;
            if (refinement > 0) {
                if (this.covers(partialIndex, refinement)) {
                    Debug.println(this.localNode.nodeID + " covers " + partialIndex + " to refinement " + refinement, "squid.ResolveDebug");
                    refineFurther = false;
                    includeLocal = true;
                }
                else {
                    BigInteger[] coords = mapping.indexToCoordinates(((ChordID) chord.getLocalID().nodeID).getIndexValue());
                    boolean included = true;
                    for (int i = 0; i < coords.length; i++) {
                        if (coords[i].compareTo(range[2*i]) < 0 || coords[i].compareTo(range[2*i+1]) > 0) {
                            included = false;
                            break;
                        }
                    }
                    if (included) {
                        includeLocal = true;
                    }
                }
            }
            if (refineFurther) {
                ClusterRefiner refiner = new ClusterRefiner(mapping);
                refiner.refine(range, refinement+1);
                // TODO: Possibly check if two or more indices can be sent to the same node
                int numDestinations = refiner.getDivisionSize();
                request = new SquidResolveRequest(numDestinations);
                Hashtable<BigInteger, SquidResolveRequest> requestTable = resolveRequests.get(resolveMessage.getResolveKey());
                if (requestTable == null) {
                    requestTable = new Hashtable<BigInteger, SquidResolveRequest>();
                    resolveRequests.put(resolveMessage.getResolveKey(), requestTable);
                }
                if (refinement == 0) {
                    requestTable.put(resolveMessage.getCallKey(), request);
                }
                int retries = RETRY_LIMIT;
                while (!request.isDone() && retries > 0) {
                    // Send requests
                    for (int i = 0; i < numDestinations; i++) {
                        BigInteger callKey = new BigInteger(64, random);
                        requestTable.put(callKey, request);
                        byte[] chordMessage = SquidMessage.newRoutingMessage(refiner.getRange(i), refiner.currentRefinement(), refiner.getIndex(i), SquidMessage.newResolveRequest(resolveMessage.getResolveKey(), callKey, localNode)).serialize();
                        chord.routeTo(chord.generateID(refiner.getIndex(i)), "squid", chordMessage);
                    }
                    // Wait for response (blocking operation)
                    if (!request.waitOn(RETRY_PERIOD, TimeUnit.MILLISECONDS)) {
                        retries--;
                    }
                }
            }
            if (refinement > 0) {
                sendResolveReply(resolveMessage, request, includeLocal);
            }
        }
        if (Debug.isEnabled("squid.debugThreadTiming")) InstrumentedThread.endInvocation("squidProcessResolveRequest");
    }

    protected void processResolveReply(int requestKey, BigInteger callKey, Vector<NodeInfo> reply) {
        SquidResolveRequest pendingRequest = resolveRequests.get(requestKey).get(callKey);
        synchronized (pendingRequest) {
            if (pendingRequest != null) {
                pendingRequest.addResult(reply);
            }
        }
    }

    protected void deliverMessage(SquidMessage msg) {
        BigInteger messageHash = new BigInteger(msg.getMessageHash());
        if (!messageHashStore.contains(messageHash)) {
            messageHashStore.add(messageHash);
            String msgTag = msg.getUserMsgTag();
            OverlayMessageListener listener = messageSubscribers.get(msgTag);
            if (listener != null) {
                if (Debug.isEnabled("squid.debugThreadTiming")) InstrumentedThread.startInvocation("squidDeliverMsg");
                listener.messageArrived(new SquidMessageCallback(msg.getDestinationKey(), msg.getMsgOrigin(), msgTag, msg.getUserMsgBytes(), this));
                if (Debug.isEnabled("squid.debugThreadTiming")) InstrumentedThread.endInvocation("squidDeliverMsg");
            }
        }
    }

    protected void sendResolveReply(SquidMessage resolveMessage, SquidResolveRequest localRequest, boolean includeLocal) {
        if (!isLeaving) {
            Vector<NodeInfo> result = new Vector<NodeInfo>();
            if (includeLocal) {
                result.add(localNode);
            }
            if (localRequest != null) {
                NodeInfo[] auxInfo = localRequest.getResult();
                for (NodeInfo node : auxInfo) {
                    result.add(node);
                }
            }
            SquidMessage response = SquidMessage.newResolveReply(resolveMessage.getResolveKey(), resolveMessage.getCallKey(), result);
            try {
                chord.sendDirect(resolveMessage.getMsgOrigin(), "squid", response.serialize());
            }
            catch (IOException ioe) {
                Debug.printStackTrace(ioe, "squid");
            }
        }
    }

    private boolean covers(BigInteger index, int refinement) {
        int idLength = myID.getSpaceDimensions() * myID.getBitLength();
        int sigBits = myID.getSpaceDimensions() * refinement; //(1 << refinement);
        BigInteger mask = BigInteger.ONE.shiftLeft(idLength-sigBits).subtract(BigInteger.ONE);
        BigInteger localIDValue = ((ChordID) chord.getLocalID().nodeID).getIndexValue();
        BigInteger comp = localIDValue.andNot(mask);
        boolean ret = !index.equals(comp);
        if (!ret && sigBits == idLength) {
            ret = (localIDValue.equals(index));
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

    //@anirudh: Loadbalancing by bitwise shift
     public BigInteger bitShift(BigInteger routingKey){
        String binaryNumTask = Integer.toBinaryString(NUM_TASKS);
        int m = BIT_LENGTH - binaryNumTask.length();
        int id = routingKey.intValue() << m;
        if((Integer.toBinaryString(id)).length() > BIT_LENGTH) {
            Debug.println(" Shift out of bounds", "LoadBalance");
            return BigInteger.valueOf(-1);
        }
        else{
            Debug.println("bitShift: "+routingKey + " " + binaryNumTask + " " + id, "LoadBalance");
            return BigInteger.valueOf(id);
        }
     }

     //@anirudh: hashing logic to perform load balancing
    public BigInteger hashIndex(BigInteger routingKey){
        BigInteger id = new BigInteger("0");
        try{
            int outputBitLength = BIT_LENGTH;
            int range = (int) java.lang.Math.pow(2,outputBitLength);
            int hashVal = Math.abs(customHash(routingKey.intValue())); // Call to the custom hash function.
            int block = range/(2*outputBitLength);   //deciding the partition size;
            int offset = hashVal%block;
            int address = counter*block+offset;
            id = BigInteger.valueOf(address);
            counter++;
            if(counter == 2*outputBitLength)
                counter = 0;
        }
        catch (Exception ex){
            System.out.println("Error in hashIndex function" + ex);
        }
        return id;
     }

     public int customHash( int seed) {
        seed = (seed+0x7ed55d16) + (seed<<12);
        seed = (seed^0xc761c23c) ^ (seed>>19);
        seed = (seed+0x165667b1) + (seed<<5);
        seed = (seed+0xd3a2646c) ^ (seed<<9);
        seed = (seed+0xfd7046c5) + (seed<<3);
        seed = (seed^0xb55a4f09) ^ (seed>>16);
        return seed;
    }

}
