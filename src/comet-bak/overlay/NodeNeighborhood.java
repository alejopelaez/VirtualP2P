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
 * NodeNeighborhood.java
 *
 * Created on September 29, 2008, 1:48 PM
 */

package tassl.automate.overlay;

import java.io.Serializable;
import programming5.arrays.ArrayOperations;

/**
 * A NodeNeighborhood object groups the information of nodes in the same neighborhood, where a neighborhood is defined as the nodes 
 * whose key distribution (the mapping of keys to nodes in the overlay) is directly affected by each other's presence/absence.
 * @author aquirozh
 */
public class NodeNeighborhood implements Serializable {
    
    private static final long serialVersionUID = 8348622856614178648L;
    
    /**
     * A node is a predecessor of the current node if its keys map to the current node if it is removed from the overlay
     */
    public static final int PREDECESSOR = 0;
    
    /**
     * The current node's successors are those for which the current node is a predecessor
     */
    public static final int SUCCESSOR = 1;
    
    protected NodeInfo[] predecessors;
    protected NodeInfo[] successors;
    
    public NodeNeighborhood(NodeInfo[] predecessorList, NodeInfo[] successorList) {
        predecessors = new NodeInfo[predecessorList.length];
        ArrayOperations.replicate(predecessorList, predecessors);
        successors = new NodeInfo[successorList.length];
        ArrayOperations.replicate(successorList, successors);
    }
    
    public NodeInfo[] getSuccessors() {
        NodeInfo[] ret = new NodeInfo[successors.length];
        ArrayOperations.replicate(successors, ret);
        return ret;
    }
    
    public NodeInfo[] getPredecessors() {
        NodeInfo[] ret = new NodeInfo[predecessors.length];
        ArrayOperations.replicate(predecessors, ret);
        return ret;
    }
    
    public NodeInfo[] get(int neighborType) {
        switch (neighborType) {
            case PREDECESSOR: return this.getPredecessors();
            case SUCCESSOR: return this.getSuccessors();
            default: throw new IllegalArgumentException("NodeNeighborhood: No such neighbor type");
        }
    }
    
    public NodeInfo getSuccessor(int index) {
        return successors[index];
    }
    
    public NodeInfo getPredecessor(int index) {
        return predecessors[index];
    }
    
    public NodeInfo get(int neighborType, int index) {
        switch (neighborType) {
            case PREDECESSOR: return this.getPredecessor(index);
            case SUCCESSOR: return this.getSuccessor(index);
            default: throw new IllegalArgumentException("NodeNeighborhood: No such neighbor type");
        }
    }
    
}
