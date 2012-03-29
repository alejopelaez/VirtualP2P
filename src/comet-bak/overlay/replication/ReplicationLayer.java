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
 * ReplicationLayer.java
 *
 * Created on September 9, 2008, 2:24 PM
 */

package tassl.automate.overlay.replication;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;
import tassl.automate.overlay.ApplicationState;
import tassl.automate.overlay.MessageCallback;
import tassl.automate.overlay.NodeInfo;
import tassl.automate.overlay.NodeNeighborhood;
import tassl.automate.overlay.OverlayID;
import tassl.automate.overlay.OverlayListener;
import tassl.automate.overlay.OverlayMessageListener;
import tassl.automate.overlay.OverlayService;

/**
 * This class defines a distributed object used by an overlay service to provide replication for application-level data. 
 * @author aquirozh
 */
public class ReplicationLayer implements OverlayListener {
    
    protected OverlayService overlay;
    protected Hashtable<String, ReplicationAwareListener> listenerTable = new Hashtable<String, ReplicationAwareListener>();
    protected Hashtable<String, Boolean> eventHandlingIndicators = new Hashtable<String, Boolean>();
    protected Hashtable<String, ApplicationState>[] stateStore;
    
    private int maxDistance;
    
    public static final String RL_TAG = "REPLICATION";
    private static boolean HANDLE_LEAVING;
    static {
        HANDLE_LEAVING = Boolean.parseBoolean(System.getProperty("overlay.replication.HANDLE_LEAVING", "true"));
    }
    
    /**
     * Creates a replication object on top of the given overlay reference to replicate data at the given number of neighbor 
     * nodes (which neighbor depends on the overlay's implementation of the getNeighborSet method)
     */
    public ReplicationLayer(OverlayService overlayRef, int numReplicationNodes) {
        overlay = overlayRef;
        if (numReplicationNodes >= 1) {
            maxDistance = numReplicationNodes+1;
            stateStore = new Hashtable[maxDistance];
            for (int i = 0; i < maxDistance; i++) {
                stateStore[i] = new Hashtable<String, ApplicationState>();
            }
            overlay.subscribeToMessages(this, RL_TAG);
        }
        else {
            throw new IllegalArgumentException("ReplicationLayer: Number of replication nodes must be at least 1");
        }
    }
    
    /**
     * Enables replication for the given listener, which must provide a state object to which replicated events
     * will be delivered.
     */
    public void replicateFor(String tag, ReplicationAwareListener listener, ApplicationState stateObject, boolean handleEvents) {
        listenerTable.put(tag, listener);
        eventHandlingIndicators.put(tag, handleEvents);
        stateStore[0].put(tag, stateObject);
        for (int i = 1; i < maxDistance; i++) {
            stateStore[i].put(tag, stateObject.createNew());
        }
    }
    
    public boolean handlingEventsFor(String tag) {
        return eventHandlingIndicators.get(tag);
    }
    
    public ApplicationState getLocalStateObject(String tag) {
        return stateStore[0].get(tag);
    }
    
    /**
     * As an overlay listener, receives messages meant for the application and replicates them. Also receives and decodes 
     * replication layer specific messages.
     */
    public void messageArrived(MessageCallback c) {
        if (!c.getMessageTag().equals(RL_TAG)) {
            doReplicateEvent(c, 0);
            listenerTable.get(c.getMessageTag()).messageArrived(c);
        }
        else {
            ReplicationMessage message = (ReplicationMessage) ReplicationMessage.createFromBytes(c.getMessageBytes());
            message.applyTo(this);
        }
    }
    
    /**
     * Called by the overlay when an application object has updated its application state.
     */
    public void replicateState(String tag, ApplicationState stateObject) {
        doReplicateState(tag, stateObject, 0);
    }
    
    public void serviceErrorOcurred(NodeInfo where, byte[] errorMessage) {
    }

    /**
     * If directed to handle leave events, will recover the local node's application state at the neighbor.
     */
    public void leaving() {
        if (HANDLE_LEAVING) {
            //>>for measuring the overhead by hjkim
            long stime, etime;
            stime = System.nanoTime()/1000;
            //<<
            
//            sendRecoverMessage(1, stateStore[maxDistance-1]);
            Hashtable<String, ApplicationState> recoveryTable = new Hashtable<String, ApplicationState>();
            for (String tag : eventHandlingIndicators.keySet()) {
                boolean handlingEvents = eventHandlingIndicators.get(tag);
                if (!handlingEvents) {
                    recoveryTable.put(tag, stateStore[0].get(tag));
                }
            }
            sendLeavingMessage(1, recoveryTable, stateStore[maxDistance-1]);

        }
    }

    /**
     * If directed to handle leave events, will save the node's application state to disk
     * TODO: Implement save and recover methods
     */
    public void terminating() {
        if (HANDLE_LEAVING) {
            // TODO: Save to disk
        }
    }

    public void newNeighbor(NodeInfo newNode) {
    }
    
    /**
     * When a new neighbor joins, the local state must be replicated at that node.
     */
    public void newNeighbor(int neighborType, NodeInfo newNode) {
        //>>for measuring the overhead by hjkim
        long stime, etime;
        stime = System.nanoTime()/1000;
        //<<

        switch (neighborType) {
            case NodeNeighborhood.SUCCESSOR: sendUpdateMessage(newNode);
            break;
            case NodeNeighborhood.PREDECESSOR: sendBackwardMessage(newNode);
            break;
        }

    }

    public void neighborDown(OverlayID absentNode) {
    }
    
    /**
     * When a neighbor fails, the replicated application state at the neighbor must be recovered and delivered to the application
     */
    public void neighborsDown(int neighborType, NodeInfo[] nodes, NodeInfo[] replacements) {
        //>>for measuring the overhead by hjkim
        long stime, etime;
        stime = System.nanoTime()/1000;
        //<<
        
        switch (neighborType) {
            case NodeNeighborhood.SUCCESSOR: sendRecoverMessage(1, stateStore[maxDistance-2]);
            break;
        }

    }
    
    // Start message handling methods
    
    protected void doReplicateEvent(MessageCallback c, int prevReplications) {
        if (stateStore[prevReplications] != null) {
            ApplicationState stateObject = stateStore[prevReplications].get(c.getMessageTag());
            if (stateObject != null) {
                stateObject.update(c);
            }
        }
        if (prevReplications < maxDistance-1) {
            sendReplicationMessage(c, prevReplications+1);
        }
    }
    
    protected void doReplicateState(String tag, ApplicationState updatedState, int prevReplications) {
        if (stateStore[prevReplications] == null) {
            stateStore[prevReplications] = new Hashtable<String, ApplicationState>();
        }
        stateStore[prevReplications].put(tag, updatedState);
        if (prevReplications < maxDistance-1) {
            sendReplicationMessage(tag, updatedState, prevReplications+1);
        }
    }
    
    protected void signalRecover(int distance, Hashtable<String, ApplicationState> stateTable) {
        //>>for measuring the overhead by hjkim
        long stime, etime;
        stime = System.nanoTime()/1000;
        //<<
        
        if (distance < maxDistance) {
            Hashtable<String, ApplicationState> tableRecovered = stateStore[distance];
            Hashtable<String, ApplicationState> tableToUpdate = stateStore[distance-1];
            if (tableRecovered != null && tableToUpdate != null) {
                for (String tag : tableRecovered.keySet()) {
                    ApplicationState stateRecovered = tableRecovered.get(tag);
                    ApplicationState stateToUpdate = tableToUpdate.get(tag);
                    if (stateRecovered != null && stateToUpdate != null) {
                        stateToUpdate.merge(stateRecovered);
                        if (distance == 1) {
                            listenerTable.get(tag).signalMergedState(stateToUpdate);
                        }
                    } 
                }
            }
            for (int i = distance+1; i < maxDistance; i++) {
                stateStore[i-1] = stateStore[i];
            }
        }
        stateStore[maxDistance-1] = stateTable;
        
        if (distance < maxDistance) {
            sendRecoverMessage(distance+1, stateStore[maxDistance-2]);
        }

    }
    
    protected void handleLeaving(int distance, Hashtable<String, ApplicationState> recoveryTable, Hashtable<String, ApplicationState> replacementTable) {
        if (distance < maxDistance) {
            Hashtable<String, ApplicationState> tableRecovered = stateStore[distance];
            Hashtable<String, ApplicationState> tableToUpdate = stateStore[distance-1];
            if (tableRecovered != null && tableToUpdate != null) {
                for (String tag : tableRecovered.keySet()) {
                    ApplicationState stateRecovered = recoveryTable.get(tag);
                    if (stateRecovered == null) {
                        stateRecovered = tableRecovered.get(tag);
                    }
                    ApplicationState stateToUpdate = tableToUpdate.get(tag);
                    if (stateRecovered != null && stateToUpdate != null) {
                        stateToUpdate.merge(stateRecovered);
                        if (distance == 1) {
                            listenerTable.get(tag).signalMergedState(stateToUpdate);
                        }
                    } 
                }
            }
            for (int i = distance+1; i < maxDistance; i++) {
                stateStore[i-1] = stateStore[i];
            }
        }
        stateStore[maxDistance-1] = replacementTable;
        
        if (distance < maxDistance) {
            sendLeavingMessage(distance+1, recoveryTable, stateStore[maxDistance-2]);
        }
    }
    
    protected void doUpdate(Vector<Hashtable<String, ApplicationState>> stateUpdate/*, Vector<Hashtable<String, Queue<MessageCallback>>> eventUpdate*/) {
        //>>for measuring the overhead by hjkim
        long stime, etime;
        stime = System.nanoTime()/1000;
        //<<

        Iterator<Hashtable<String, ApplicationState>> stateIterator = stateUpdate.iterator();
        int distance = 1;
        while (stateIterator.hasNext()) {
            stateStore[distance++] = stateIterator.next();
        }
        sendInsertMessage(1);

    }
    
    protected void doInsert(int distance) {
        for (int i = maxDistance-1; i > distance; i--) {
            stateStore[i] = stateStore[i-1];
        }
        stateStore[distance] = new Hashtable<String, ApplicationState>();
        for (String tag : stateStore[0].keySet()) {
            stateStore[distance].put(tag, stateStore[0].get(tag).createNew());
        }
        if (distance < maxDistance-1) {
            sendInsertMessage(distance+1);
        }
    }
    
    protected void doBackwardAccept(Hashtable<String, ApplicationState> stateTable) {
        //>>for measuring the overhead by hjkim
        long stime, etime;
        stime = System.nanoTime()/1000;
        //<<
        
        for (String tag : stateTable.keySet()) {
            ApplicationState currentState = stateStore[0].get(tag);
            if (currentState == null) {
                stateStore[0].put(tag, stateTable.get(tag));
            }
            else {
                currentState.merge(stateTable.get(tag));
            }
            listenerTable.get(tag).signalMergedState(currentState);
        }

    }
    
    // Start private utility methods
    
    private void sendReplicationMessage(MessageCallback c, int numReplications) {
        ReplicationMessage message = ReplicationMessage.newEventMessage(c, numReplications);
        for (NodeInfo successor : overlay.getNeighborhood().getSuccessors()) {
            try {
                overlay.sendDirect(successor, RL_TAG, message.serialize());
            }
            catch (IOException ioe) {
                System.out.println("ReplicationLayer: Could not send replication message for " + successor.nodeURI + ": " + ioe.getMessage());
            }
        }
    }
    
    private void sendReplicationMessage(String tag, ApplicationState stateObject, int numReplications) {
        ReplicationMessage message = ReplicationMessage.newStateMessage(tag, stateObject, numReplications);
        for (NodeInfo successor : overlay.getNeighborhood().getSuccessors()) {
            try {
                overlay.sendDirect(successor, RL_TAG, message.serialize());
            }
            catch (IOException ioe) {
                System.out.println("ReplicationLayer: Could not send replication message for " + successor.nodeURI + ": " + ioe.getMessage());
            }
        }
    }
    
    private void sendRecoverMessage(int distance, Hashtable<String, ApplicationState> stateTable) {
        ReplicationMessage message = ReplicationMessage.newRecoverMessage(distance, stateTable);
        for (NodeInfo successor : overlay.getNeighborhood().getSuccessors()) {
            try {
                overlay.sendDirect(successor, RL_TAG, message.serialize());
            }
            catch (IOException ioe) {
                System.out.println("ReplicationLayer: Could not send recovery message for " + successor.nodeURI + ": " + ioe.getMessage());
            }
        }
    }
    
    private void sendLeavingMessage(int distance, Hashtable<String, ApplicationState> recoveryTable, Hashtable<String, ApplicationState> replacementTable) {
        ReplicationMessage message = ReplicationMessage.newLeavingMessage(distance, recoveryTable, replacementTable);
        for (NodeInfo successor : overlay.getNeighborhood().getSuccessors()) {
            try {
                overlay.sendDirect(successor, RL_TAG, message.serialize());
            }
            catch (IOException ioe) {
                System.out.println("ReplicationLayer: Could not send leaving message to: " + successor.nodeURI + ": " + ioe.getMessage());
            }            
        }
    }
    
    private void sendUpdateMessage(NodeInfo newNode) {
        ReplicationMessage message = ReplicationMessage.newUpdateMessage(createStateUpdateVector()/*, createEventUpdateVector()*/);
        try {
            overlay.sendDirect(newNode, RL_TAG, message.serialize());
        }
        catch (IOException ioe) {
            System.out.println("ReplicationLayer: Could not send update message: " + ioe.getMessage());
        }
    }
    
    private void sendInsertMessage(int distance) {
        ReplicationMessage message = ReplicationMessage.newInsertMessage(distance);
        for (NodeInfo successor : overlay.getNeighborhood().getSuccessors()) {
            try {
                overlay.sendDirect(successor, RL_TAG, message.serialize());
            }
            catch (IOException ioe) {
                System.out.println("ReplicationLayer: Could not send insert message for " + successor.nodeURI + ": " + ioe.getMessage());
            }
        }
    }
    
    private void sendBackwardMessage(NodeInfo newNode) {
        Hashtable<String, ApplicationState> backwardTable = new Hashtable<String, ApplicationState>();
        for (String tag : stateStore[0].keySet()) {
            ApplicationState splitState = stateStore[0].get(tag).split(newNode.nodeID);
            if (splitState != null) {
                backwardTable.put(tag, splitState);
            }
        }
        if (!backwardTable.isEmpty()) {
            ReplicationMessage message = ReplicationMessage.newBackwardMessage(backwardTable);
            try {
                overlay.sendDirect(newNode, RL_TAG, message.serialize());
            }
            catch (IOException ioe) {
                System.out.println("ReplicationLayer: Could not send backward message for " + newNode.nodeURI + ": " + ioe.getMessage());
            }
        }
    }
    
    private Vector<Hashtable<String, ApplicationState>> createStateUpdateVector() {
        Vector<Hashtable<String, ApplicationState>> ret = new Vector<Hashtable<String, ApplicationState>>();
        for (int i = 0; i < maxDistance-1; i++) {
            ret.add(stateStore[i]);
        }
        return ret;
    }

//    public void replicateMessageArrived(MessageCallback c) {
//    }

    public OverlayMessageListener createNew(OverlayService serviceRef) {
        return new ReplicationLayer(serviceRef, maxDistance-1);
    }
    
}
