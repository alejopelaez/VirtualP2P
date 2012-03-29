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

import java.io.Serializable;
import java.math.BigInteger;
import programming5.collections.RotatingVector;
import programming5.io.Debug;
import programming5.io.InstrumentedThread;
import programming5.net.MalformedMessageException;
import programming5.net.Message;

/**
 * Stores and manages the network references of known overlay nodes, according to the chord specification. 
 * Can be serialized to be sent over the network. 
 * @author ahernandez
 */
public class FingerTable implements Serializable {
    
    private static final long serialVersionUID = -2605914076737089074L;
    
    /**
     * The possible neighborhood states of the node on the ring, respectively, when the node has no neighbors, a single neighbor, or
     * more than one. Useful to distinguish special cases where few nodes exist.
     */
    public enum RingState {SINGLE, PAIR, CHORD};
    
    /**
     * Maximum number of references to keep; by default, it is the number of bits used to encode the ID
     * space.
     */
    protected final int TABLE_SIZE;
    
    /**
     * Number of redundant (backup) successors to store, when available.
     */
    protected final int REDUNDANCY;
    
    protected ChordNodeInfo[] node;
    protected ChordNodeInfo[] redundant;
    protected RotatingVector<ChordNodeInfo> victimBuffer = null;
    protected ChordID[] start;
    protected ChordNodeInfo localNode;
    
    private int longestFinger = 0;
    
    /**
     * Creates an empty finger table for the local node.
     * @param localID chord id of the local node
     * @param localURI physical address of the local node
     */
    public FingerTable(ChordID localID, String localURI, int redundancy) {
        TABLE_SIZE = localID.NUM_BITS;
        REDUNDANCY = redundancy;
        localNode = new ChordNodeInfo(localID, localURI);
        node = new ChordNodeInfo[TABLE_SIZE+1]; // Holds predecessor [0], successor [1], and finger [2-TABLE_SIZE] references
        for (int i = 0; i < node.length; i++) {
            node[i] = localNode;
        }
        redundant = new ChordNodeInfo[REDUNDANCY]; // Holds backup successor references
        start = new ChordID[TABLE_SIZE+2]; // Holds the limits of finger intervals
        start[0] = localID;
        for (int i = 1; i < start.length; i++) {
            start[i] = start[0].fingerStep(i-1);
        }
        if (redundancy > 0) {
            victimBuffer = new RotatingVector<ChordNodeInfo>(redundancy);
        }
    }
    
    /**
     * Fills current table based on a given finger table. Although it should be received from a neighbor
     * (successor), it does not have to be.
     * @param initializer existing finger table from a remote node
     */
    public void initialize(FingerTable initializer) {
//        InstrumentedThread.startInvocation("FingerTable.initialize");
        this.update(initializer.getLocalNode());
        if (initializer.predecessor() != null) {
            this.update(initializer.predecessor());
        }
        for (int i = 1; i <= initializer.longestFinger; i++) {
            this.update(initializer.node(i));
        }
//        InstrumentedThread.endInvocation("FingerTable.initialize");
    }
    
    /**
     *Fills current table based on string representation of a finger table.
     *@param initializerString string representation of finger table, preferably obtained by @link{#toString}
     */
    public void initialize(String initializerString) throws IllegalArgumentException {
        try {
//            InstrumentedThread.startInvocation("FingerTable.initialize");
            Message decoder = new Message(initializerString);
            if (decoder.getHeader().equals("CHFT")) {
                for (int i = 0; i < decoder.getMessageSize(); i++) {
                    this.update(new ChordNodeInfo(decoder.getMessageItem(i)));
                }
            }
//            InstrumentedThread.endInvocation("FingerTable.initialize");
        }
        catch (MalformedMessageException mme) {
            throw new IllegalArgumentException("FingerTable: Could not initialize: Invalid initializer string");
        }
    }
    
    /**
     * @return ith finger (0 being the predecessor, 1 the successor)
     */
    public ChordNodeInfo node(int i) {
        return node[i];
    }
    
    /**
     * @return current successor
     */
    public synchronized ChordNodeInfo successor() {
        return node[1];
    }
    
    /**
     * @param shift the number of the redundant successor to return with successor(0) != successor()
     * @return one of the available successors, given the level of redundancy used
     */
    public ChordNodeInfo successor(int shift) {
        return redundant[shift];
    }
    
    /**
     * @return current predecessor
     */
    public synchronized ChordNodeInfo predecessor() {
        return node[0];
    }
    
    /**
     * @return the closest entry that precedes or equals the given node id, clockwise from the local ID; If the given nodeID is equal to the predecessor ID, then returns null
     */
    public synchronized ChordNodeInfo closestPrecedingFinger(ChordID nodeID) {
        if (Debug.isEnabled("chord.debugThreadTiming")) InstrumentedThread.startInvocation("FingerTable.closestPrecedingFinger");
        ChordNodeInfo ret = localNode;
        if (this.predecessor() != null) {
            if (nodeID.isBetweenIntervalRight(this.predecessor().getChordID(), localNode.getChordID()) || localNode.nodeID.equals(this.predecessor().nodeID)) {
                ret = this.predecessor();
            } 
            else {
                if (nodeID.equals(this.predecessor().getChordID())) {
                    ret = null;
                }
                else {
                    for (int i = longestFinger; i > 0; i--) {
                        if (node[i].getChordID().isBetweenIntervalRight(localNode.getChordID(), nodeID)) {
                            ret = node[i];
                            break;
                        }
                    }
                }
            }
            // Check victim buffer
            if (ret != null) {
                ChordNodeInfo alt = victimBufferFind(nodeID);
                if (alt != null) {
                    BigInteger retDiff = nodeID.ringDifference(ret.getChordID());
                    BigInteger altDiff = nodeID.ringDifference(alt.getChordID());
                    if (altDiff.compareTo(retDiff) < 0) {
                        ret = alt;
                    }
                }
            }
        }
        if (Debug.isEnabled("chord.debugThreadTiming")) InstrumentedThread.endInvocation("FingerTable.closestPrecedingFinger");
        return ret;
    }

    /**
     * @return the closest entry that strictly precedes the given node id, clockwise from the local ID
     */
    public synchronized ChordNodeInfo closestStrictlyPrecedingFinger(ChordID nodeID) {
        if (Debug.isEnabled("chord.debugThreadTiming")) InstrumentedThread.startInvocation("FingerTable.closestStrictlyPrecedingFinger");
        ChordNodeInfo ret = localNode;
        if (this.predecessor() != null) {
            if (nodeID.isBetweenIntervalRight(this.predecessor().getChordID(), localNode.getChordID()) || localNode.nodeID.equals(this.predecessor().nodeID)) {
                ret = this.predecessor();
            } 
            else {
                for (int i = longestFinger; i > 0; i--) {
                    if (node[i].getChordID().isBetween(localNode.getChordID(), nodeID)) {
                        ret = node[i];
                        break;
                    }
                }
            }
        }
        if (Debug.isEnabled("chord.debugThreadTiming")) InstrumentedThread.endInvocation("FingerTable.closestStrictlyPrecedingFinger");
        return ret;
    }
    
    /**
     * Inserts or replaces entries in the table with the given entry, according to the chord
     * specification. The method will automatically determine, based on the existing entries, if the new
     * entry should be placed as predecessor, successor, or a specific finger; if the entry already
     * exists in the table, the method will do nothing.
     * @param entry the info of the node that might be kept as a finger table reference
     * @return true if the table was updated with the given entry
     */
    public synchronized boolean update(ChordNodeInfo entry) {
        if (Debug.isEnabled("chord.debugThreadTiming")) InstrumentedThread.startInvocation("FingerTable.update");
        boolean updated = false;
        if (!entry.getChordID().equals(localNode.nodeID)) {
            ChordNodeInfo toRedundant = entry;
            int i = 0;
            do {
                i++;
                if (node[i].nodeID.equals(localNode.nodeID) || entry.getChordID().isBetween(localNode.getChordID(), node[i].getChordID())) {
                    victimBufferInsert(node[i]);
                    node[i] = entry;
                    updated = true;
                }
            } while (!entry.getChordID().isBetweenIntervalLeft(start[i], start[i+1]));
            
            if (i > longestFinger) {
                longestFinger = i;
            }
            
            if (!updated) {
                for (int r = 0; r < REDUNDANCY; r++) {
                    if (redundant[r] != null) {
                        if (entry.getChordID().isBetween(localNode.getChordID(), redundant[r].getChordID())) {
                            for (int j = REDUNDANCY-2; j >= r; j--) {
                                redundant[j+1] = redundant[j];
                            }
                            redundant[r] = entry;
                        }
                    }
                    else {
                        break;
                    }
                }
            }
            
            if (this.predecessor().nodeID.equals(localNode.nodeID) || entry.getChordID().isBetween(this.predecessor().getChordID(), localNode.getChordID())) {
                victimBufferInsert(node[0]);
                node[0] = entry;
                updated = true;
            }
        }
        if (updated && Debug.isEnabled("chord.FingerTable")) {
            System.out.println("FingerTable: Updated with node " + entry);
            this.print();
        }
        if (Debug.isEnabled("chord.debugThreadTiming")) InstrumentedThread.endInvocation("FingerTable.update");
        return updated;
    }
    
    /**
     * Removes the node with the given id from the table, if it exists, replacing with other entries
     * when possible (e.g. next successor if current successor is deleted)
     * @param nodeID id of the node to delete from the table
     * @return the info of the node deleted (null if no node was deleted)
     */
    public synchronized int deleteNode(ChordID nodeID) {
        if (Debug.isEnabled("chord.debugThreadTiming")) InstrumentedThread.startInvocation("FingerTable.deleteNode");
        boolean deleted = false;
        int deletedNode = -1;
        if (longestFinger >= 0 && !nodeID.equals(localNode.nodeID)) {   // Valid table
            // Check longestFinger
            if (node[longestFinger].nodeID.equals(nodeID)) {
                node[longestFinger--] = localNode;
                deleted = true;
                while (longestFinger > 1 && node[longestFinger].nodeID.equals(nodeID)) {
                    node[longestFinger--] = localNode;
                }
                if (longestFinger == 1 && !node[0].nodeID.equals(nodeID)) {
                    node[longestFinger] = node[0];
                } 
                else {
                    node[longestFinger] = localNode;
                }
                deletedNode = longestFinger;
            } 
            else {
                // If not longestFinger, another one of the fingers
                for (int i = longestFinger-1; i > 0; i--) {
                    if (node[i].nodeID.equals(nodeID)) {
                        node[i] = node[i+1];
                        deleted = true;
                        while (node[--i].nodeID.equals(nodeID)) {
                            node[i] = node[i+1];
                        }
                        deletedNode = i;
                        break;
                    }
                }
            }
        
            // Check if node exists in redundant nodes
            for (int r = 0; r < REDUNDANCY; r++) {
                if (redundant[r] != null) {
                    if (redundant[r].nodeID.equals(nodeID)) {
                        for (int j = r; j < REDUNDANCY-1; j++) {
                            redundant[j] = redundant[j+1];
                        }
                        redundant[REDUNDANCY-1] = null;
                        break;
                    }
                }
            }
            if (deleted) { // If deleted, check if better successor exists in redundant nodes
                for (int r = 0; r < REDUNDANCY; r++) {
                    if (redundant[r] != null) {
                        if (update(redundant[r])) {
                            for (int j = r; j < REDUNDANCY-1; j++) {
                                redundant[j] = redundant[j+1];
                            }
                            redundant[REDUNDANCY-1] = null;
                            break;
                        }
                    } 
                    else {
                        break;
                    }
                }
            } 
            // Check predecessor
            if (node[0].nodeID.equals(nodeID)) {
                node[0] = node[longestFinger];
            }
        }
        if (deleted && Debug.isEnabled("chord.FingerTable")) {
            System.out.println("FingerTable: Deleted node " + nodeID);
            this.print();
        }
        if (Debug.isEnabled("chord.debugThreadTiming")) InstrumentedThread.endInvocation("FingerTable.deleteNode");
        return deletedNode;
    }
    
    /**
     * @return the id of the local node
     */
    public ChordNodeInfo getLocalNode() {
        return localNode;
    }
    
    protected ChordNodeInfo[] getKnownNodeInfo() {
        int numRedundant = 0;
        if (REDUNDANCY > 0) {
            while (redundant[numRedundant] != null) {
                numRedundant++;
                if (numRedundant == REDUNDANCY) {
                    break;
                }
            }
        }
        ChordNodeInfo[] ret;
        int returnIndex = 0;
        if (node[longestFinger].equals(node[0])) {
            ret = new ChordNodeInfo[longestFinger + numRedundant];
        }
        else {
            ret = new ChordNodeInfo[longestFinger + numRedundant + 1];
            ret[returnIndex++] = node[0];
        }
        for (int i = 1; i <= longestFinger; i++) {
            ret[returnIndex++] = node[i];
        }
        for (int i = 0; i < numRedundant; i++) {
            ret[returnIndex++] = redundant[i];
        }
        return ret;
    }
    
    /**
     *@return the special case in terms of the type of neighborhood of the node; useful for handling the special cases when there are 
     *few (none or one) neighbors. 
     */
    public RingState getRingState() {
        RingState ret;
        if (longestFinger == 0) {
            ret = RingState.SINGLE;
        }
        else if (node[0].getChordID().equals(node[1].getChordID())) {
            ret = RingState.PAIR;
        }
        else {
            ret = RingState.CHORD;
        }
        return ret;
    }
    
    public synchronized String toString() {
        Message encoder = new Message();
        encoder.setHeader("CHFT");
        encoder.addMessageItem(localNode.toString());
        ChordNodeInfo aux = localNode;
        for (int i = 0; i <= longestFinger; i++) {
            if (!node[i].nodeID.equals(aux.nodeID)) {
                encoder.addMessageItem(node[i].toString());
                aux = node[i];
            }
        }
        String ret = null;
        try {
            ret = encoder.getMessage();
        }
        catch (MalformedMessageException mme) {mme.printStackTrace();}
        return ret;
    }
    
    public void print() {
        System.out.println("\nNode FT: " + localNode.toString());
        ChordNodeInfo aux = localNode;
        for (int i = 0; i <= longestFinger; i++) {
            if (!node[i].nodeID.equals(aux.nodeID)) {
                System.out.println(node[i].toString());
                aux = node[i];
            }
        }
    }

    protected void victimBufferInsert(ChordNodeInfo entry) {
        if (victimBuffer != null) {
            if (!victimBuffer.contains(entry)) {
                victimBuffer.add(entry);
            }
        }
    }

    protected ChordNodeInfo victimBufferFind(ChordID nodeID) {
        ChordNodeInfo ret = null;
        if (victimBuffer != null) {
            BigInteger distance = null;
            for (ChordNodeInfo entry : victimBuffer) {
                if (entry.getChordID().isBetweenIntervalRight(localNode.getChordID(), nodeID)) {
                    if (ret == null)  {
                        ret = entry;
                        distance = nodeID.ringDifference(entry.getChordID());
                    }
                    else {
                        BigInteger newDistance = nodeID.ringDifference(entry.getChordID());
                        if (newDistance.compareTo(distance) < 0) {
                            ret = entry;
                            distance = newDistance;
                        }
                    }
                }
            }
        }
        return ret;
    }
    
}
