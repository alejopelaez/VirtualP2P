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
 * ChordNodeNeighborhood.java
 *
 * Created on September 29, 2008, 2:43 PM
 */

package tassl.automate.overlay.chord;

import java.util.Map;
import programming5.arrays.ArrayOperations;
import programming5.collections.MultiVector;
import tassl.automate.overlay.NodeInfo;
import tassl.automate.overlay.NodeNeighborhood;

/**
 * NodeNeighborhood that includes neighbor categories that are specific to the chord overlay
 * @author aquirozh
 */
public class ChordNodeNeighborhood extends NodeNeighborhood {
    
    /**
     * Remote successors indicate nodes to which a key corresponding to the current node would be mapped in a remote group of a 
     * two-level chord overlay.
     */
    public static final int REMOTE_SUCCESSOR = 2;
    
    protected MultiVector<String, ChordNodeInfo> remoteSuccessors;
    
    public ChordNodeNeighborhood(ChordNodeInfo predecessor, ChordNodeInfo successor, Map<String, ChordNodeInfo> remoteSuccessorList) {
        super(new NodeInfo[] {predecessor}, new NodeInfo[] {successor});
        remoteSuccessors = new MultiVector<String, ChordNodeInfo>(remoteSuccessorList);
    }
    
    public ChordNodeInfo getSuccessor() {
        return (ChordNodeInfo) successors[0];
    }
    
    public ChordNodeInfo getPredecessor() {
        return (ChordNodeInfo) predecessors[0];
    }
    
    /**
     * @param neighborType integer constants defined in NodeNeighborhood
     */
    public ChordNodeInfo getLocal(int neighborType) {
        return (ChordNodeInfo) super.get(neighborType)[0];
    }
    
    /**
     * Method specific to two-level chord when more than one ring is in place
     * @return the successors of the local node in the remote rings
     */
    public ChordNodeInfo[] getRemoteSuccessors() {
        ChordNodeInfo[] ret = new ChordNodeInfo[remoteSuccessors.size()];
        ArrayOperations.replicate(remoteSuccessors.second().toArray(), ret);
        return ret;
    }
    
    /**
     * @param neighborType integer constants defined in NodeNeighborhood and ChordNodeNeighborhood
     */
    @Override
    public NodeInfo[] get(int neighborType) {
        NodeInfo[] ret = null;
        try {
            ret = super.get(neighborType);
        }
        catch (IllegalArgumentException iae) {
            switch (neighborType) {
                case REMOTE_SUCCESSOR: ret = (NodeInfo[]) this.getRemoteSuccessors();
                break;
                default: throw iae;
            }
        }
        finally {
            return ret;
        }
    }
    
    /**
     * Method specific to two-level chord when more than one ring is in place
     * @return the successor of the local node in the remote ring given by the cluster identifier
     */
    public ChordNodeInfo getRemoteSuccessor(String cluster) {
        return remoteSuccessors.getInSecond(cluster);
    }
    
    /**
     * Method specific to two-level chord when more than one ring is in place
     * @return the successors of the local node in the remote rings, indexed by the cluster name of each ring
     */
    public Map<String, ChordNodeInfo> getRemoteSuccessorMap() {
        return remoteSuccessors.clone();
    }
    
    /**
     * @param neighborType integer constants defined in NodeNeighborhood and ChordNodeNeighborhood
     * @param index the neighbor index as defined by the particular overlay
     */
    @Override
    public NodeInfo get(int neighborType, int index) {
        NodeInfo ret = null;
        try {
            ret = super.get(neighborType, index);
        }
        catch (IllegalArgumentException iae) {
            switch (neighborType) {
                case REMOTE_SUCCESSOR: ret = remoteSuccessors.getInSecondAt(index);
                break;
                default: throw iae;
            }
        }
        finally {
            return ret;
        }
    }
    
}
