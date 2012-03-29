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

import java.io.IOException;
import java.util.List;
import tassl.automate.overlay.replication.ReplicationAwareListener;
import tassl.automate.overlay.management.LoadManager;

/**
 * An overlay service is responsible for defining and autonomically managing the connections between devices on 
 * a physical network, and for routing between them using a particular identifier scheme. Its 
 * autonomic behavior consists in maintaining said connectivity and routing in the face of device 
 * connections/disconnections and/or failures.
 * @author ahernandez
 */
public interface OverlayService<E extends OverlayID> extends OverlayObject {
	
	/**
	 * Calls for the node to join the overlay, which will be done differently for different overlays.
	 * @param uri the identifier of the node on the physical network in the form of a uri
	 * @return the ID given to the node on the overlay
	 */
	public E join(String uri) throws IOException; 
	
	/**
	 * Calls for the node to leave the overlay. Specific overlay implementations should handle these departures 
	 * to maintain routing capability within the overlay
	 */
	public void leave();

        /**
         * Calls for the node to terminate its connection to the overlay, without making an effort to
         * maintain the routing capability within the overlay. Meant to be used to dissolve the entire
         * overlay.
         * TODO: Change functionality to terminate entire ring with a single call
         */
        public void terminate();
        
        /**
         * Calls for the specific overlay implementation to create an overlay specific OverlayID object. The variable parameters 
         * will depend on the specific overlay implementation
         */
        public E generateID(Object... parameters);
	
	/**
	 * Calls for the overlay to send the given payload to the listener subscribed with the given tag on the 
	 * peer identified by or responsible for the given ID
	 * @param peer the destination
	 * @param tag the id within the peer of a listening service or application on top of the overlay; if null, 
	 * the message will be delivered to all listeners on the remote peer(s).
	 * @param payload a message to send to the destination
	 */
	public void routeTo(E peer, String tag, byte[] payload);
	
	/**
	 * Calls for the overlay to send the given payload to the listener subscribed with the given tag on a 
	 * group of peers identified by or responsible for the given IDs. 
	 * May or may not have a different implementation from routeTo an individual peer, depending on whether 
	 * the specific overlay can take advantage of grouping or ordering the given peers.
	 * @param peers an array of destination peer IDs
	 * @param tag the id within the peer of a listening service or application on top of the overlay; if 
	 * null, the message will be delivered to all listeners on remote peer(s)
	 * @param payload a message to send to all of the destination peers
	 */
	public void routeTo(List<E> peers, String tag, byte[] payload);
	
	/**
	 * Calls for the overlay to ensure delivery of the given payload to the listener subscribed with 
	 * the given tag on the peer identified by or responsible for the given ID
	 * @param peer the destination
	 * @param tag the id within the peer of a listening service or application on top of the overlay; if null, 
	 * the message will be delivered to all listeners on the remote peer(s).
	 * @param payload a message to send to the destination
	 */
	public void reliableRouteTo(E peer, String tag, byte[] payload) throws IOException;
	
	/**
	 * Calls for the overlay to ensure delivery of the given payload to the listener subscribed with 
	 * the given tag on a group of peers identified by or responsible for the given IDs. 
	 * May or may not have a different implementation from routeTo an individual peer, depending on whether 
	 * the specific overlay can take advantage of grouping or ordering the given peers.
	 * @param peers an array of destination peer IDs
	 * @param tag the id within the peer of a listening service or application on top of the overlay; if 
	 * null, the message will be delivered to all listeners on remote peer(s)
	 * @param payload a message to send to all of the destination peers
	 */
	public void reliableRouteTo(List<E> peers, String tag, byte[] payload) throws IOException;
	
	/**
	 * Calls for the overlay to find the physical address of the given peer or peers responsible for 
	 * the given ID.
	 * @param peer ID to resolve
	 * @return object(s) that contains the requested information
	 */
	public NodeInfo[] resolve(E peer) throws ResolveException;
        
        /**
         * Sends a message directly to the given node, bypassing the overlay's routing protocol
         */
        public void sendDirect(NodeInfo node, String tag, byte[] payload) throws IOException;
	
	/**
	 * @return the contact of the local node, obtained when joining the overlay
	 */
	public NodeInfo getLocalID();
	
	/**
	 * An application may want to implement different routing schemes (e.g. gossip) on the given overlay, 
	 * for which the set of neighbors, determined by the particular overlay structure, is needed.  
	 * @return the set of IDs of known peers in the overlay
	 */
	public NodeInfo[] getNeighborSet();
        
        /**
         * A node neighborhood is a subset of the full set of neighbors (reachable nodes) that affect each other's key mapping (the 
         * set of keys or overlay id's that map to them).
         * @return a NodeNeighborhood object that classifies neighbors into different types (e.g. successors, predecessors), 
         * according to their relationship in terms of the key mapping.
         */
        public NodeNeighborhood getNeighborhood();
        
	/**
	 * @param listener application or service that wishes to receive messages and events from the overlay
	 * @param tag identifies the type of messages that the listener is interested in
	 */
	public void subscribe(OverlayListener listener, String tag);
        
        /**
         * Used by an application or service that wishes to receive messages from the overlay and have the overlay manage its state 
         * for dynamic joins and leaves, as well as be able to replicate its state on demand. The recommended semantics for the use of 
         * the application state when subscribing to the overlay in this way is to pass a reference of the primary state managed at 
         * the application level. The overlay will not update this state object, except when it is necessary to merge it with a remote 
         * state (because of failure or leaving of a node), so it is the application's responsibility to do so when messages are 
         * received. It is also the application's responsibility to call for the replication of its state object using the overlay's 
         * updateApplicationState method, except when leave is called. In the latter case, the overlay will take care of copying the 
         * current state and merging it with the state at a neighboring node.
         * @param listener application or service that will receive messages from the overlay
         * @param tag identifies the type of messages that the listener is interested in
         * @param stateObject the reference to the application state that will be managed by the overlay to handle joins and leaves
         */
        public void subscribe(ReplicationAwareListener listener, String tag, ApplicationState stateObject);
        
        /**
	 * @param listener application or service that no longer wishes to receive messages or events from the 
	 * overlay
	 * @param tag identifies the type of messages that the listener is no longer interested in
	 */
	public void unsubscribe(OverlayListener listener, String tag);
	
	/**
	 * @param listener application or service that wishes to receive only messages from the overlay
	 * @param tag identifies the type of messages that the listener is interested in
	 */
	public void subscribeToMessages(OverlayMessageListener listener, String tag);
        
        /**
         * Used by an application or service that wishes to receive messages from the overlay and have those messages replicated 
         * in an application state object for recovery upon failure. The recommended semantics for the use of the application state 
         * when subscribing to messages in this way is to pass a copy of the primary state managed at the application level that can 
         * be updated independently of each other. An update to the replicated state copy should perform no external actions that 
         * are carried out by the application upon receiving messages, such as signaling other objects or sending a reply.
         * @param listener application or service that will receive messages from the overlay
         * @param tag identifies the type of messages that the listener is interested in
         * @param stateObject the application state that will be replicated by the overlay and updated across nodes each time that 
         * a message is received
         */
        public void subscribeToMessages(ReplicationAwareListener listener, String tag, ApplicationState stateObject);
	
	/**
	 * @param listener application or service that no longer wishes to receive messages from the 
	 * overlay
	 * @param tag identifies the type of messages that the listener is no longer interested in; if null, 
	 * the listener will be completely removed 
	 */
	public void unsubscribeFromMessages(OverlayMessageListener listener, String tag);
	
	/**
	 * @param listener application or service that wishes to receive only events from the overlay
	 */
	public void subscribeToEvents(OverlayStructureListener listener);
	
	/**
	 * @param listener application or service that no longer wishes to receive events from the 
	 * overlay
	 */
	public void unsubscribeFromEvents(OverlayStructureListener listener);
        
        /**
         * Informs the overlay that the application state has changed externally (by events other 
         * than messages received through the overlay). A replication aware listener should have previously 
         * subscribed for the given tag; otherwise, calling this method will have no effect.
         */
        public void updateApplicationState(String tag, ApplicationState stateObject);
        
        /**
         * A virtual node is an overlay service instance that joins with the given ID and whose existence is subordinated to 
         * that of the creating overlay instance. A virtual node may be created on a different physical machine than that of 
         * its parent (creating) node.
         */
        public void createVirtualNode(E nodeID);
        
        /**
         * A load manager monitors application state and in responsible for creating and/or terminating virtual nodes in 
         * order to keep a balance of load on the physical nodes that make up the overlay.
         */
        public void attachLoadManager(LoadManager manager);
	
}
