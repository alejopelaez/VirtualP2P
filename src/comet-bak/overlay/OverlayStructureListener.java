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

package tassl.automate.overlay;

/**
 * Identifies a service that is only interested in structural changes in the overlay (i.e. node 
 * connections/disconnections).
 * @author ahernandez
 */
public interface OverlayStructureListener {

	/**
	 * Gives the listener a chance to perform tasks (e.g. offload data to another peer) when its node 
	 * leaves the overlay
	 */
	public void leaving();

        /**
         * Different from leaving in that the listener should not count on the ring structure to be
         * maintained after the node leaves the overlay (e.g. the ring is dissolving). Persistence
         * operations or final cleanup actions only should be performed.
         */
        public void terminating();
	
	/**
	 * Gives the listener a chance to perform tasks (e.g. load balancing) when a new node is recognized as 
	 * a neighbor in the overlay.
	 * @param newNode contact information of the node that has recently joined
         * @deprecated this method should no longer be called by overlay service implementations, and will not be called by chord and 
         * squid. Listeners should implement @link{#newNeighbor(int)} instead.
	 */
	public void newNeighbor(NodeInfo newNode);
        
        /**
         * Gives the listener a chance to perform tasks (e.g. load balancing) when a new node is recognized as 
	 * a neighbor in the overlay.
	 * @param neighborType the type of neighbor that joined (e.g. successor/predecessor), as defined in @link{NodeNeighborhood}
         * @param newNode the contact info of the node that joined
         */
        public void newNeighbor(int neighborType, NodeInfo newNode);
	
	/**
	 * Gives the listener a chance to perform tasks (e.g. repair and recovery) when a node absence 
	 * is detected at a neighboring node
	 * @param absentNode recently unresponsive node
         * @deprecated this method should no longer be called by overlay service implementations, and will not be called by chord and 
         * squid. Listeners should implement @link{#neighborDown(int)} instead.
	 */
	public void neighborDown(OverlayID absentNode);
        
        /**
         * Gives the listener a chance to perform tasks (e.g. repair and recovery) when a node absence 
	 * is detected at a neighboring node
         * @param neighborType the type of neighbor(s) that failed (e.g. successor/predecessor), as defined in @link{NodeNeighborhood}
         * @param nodes the (broken) contact info for the nodes that failed
	 * @param neighborhoodPositions the previous positions of the absent nodes in the neighborhood object returned by the overlay 
         * service; useful for identifying the replacement nodes for the nodes that failed
         */
        public void neighborsDown(int neighborType, NodeInfo[] failedNodes, NodeInfo[] replacements);
	
}
