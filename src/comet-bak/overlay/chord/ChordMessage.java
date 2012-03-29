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
import java.util.Vector;

import tassl.automate.util.MessageObject;

/**
 * Serializable object to encode and interpret messages in chord
 * @author ahernandez
 */
public class ChordMessage extends MessageObject {
    
    private static final long serialVersionUID = 5283413279328086064L;
                                                       
    // Message codes
    private static final int JOIN_REQUEST = 1;
    private static final int JOIN_REPLY = -1;
    private static final int STABILIZE_REQUEST = 2;
    private static final int STABILIZE_REPLY = -2;
    private static final int NOTIFY = 3;
    private static final int ROUTE = 4;
    private static final int RESOLVE_REQUEST = 5;
    private static final int RESOLVE_REPLY = -5;
    private static final int LEAVING = 6;
    private static final int REPAIR = 7;
    private static final int PING = 8;
    private static final int PING_ACK = -8;
    private static final int CREATE_VIRTUAL = 9;
    private static final int ACK_VIRTUAL = -9;
    private static final int VIRTUAL_LEAVE_UP = 10;
    private static final int VIRTUAL_LEAVE_DOWN = 11;
    private static final int NODE_UPDATE = 12;
    
        /*
         * Hidden constructor to call the MessageObject constructor with message codes as types and specific
         * payloads
         */
    private ChordMessage(int type, Serializable[] payload) {
        super(type, payload);
    }
    
    /**
     * Creates a join message, sent by a node that wishes to join the network
     * @param newNode the id of the new node
     * @param nodeUri the physical address of the new node
     * @param broadcast true if the message is being broadcast to the network, false if using a single
     * bootstrap node
     * @return MessageObject of the correct type and payload
     */
    public static ChordMessage newJoinMessage(ChordID newNode, String nodeUri, boolean broadcast) {
        //System.out.println("Just in case 1: " + newNode);
    	Serializable[] payload = new Serializable[3];
        payload[0] = newNode;
        //System.out.println("Just in case 2: " + (ChordID) payload[0]);
        payload[1] = nodeUri;
        payload[2] = new Boolean(broadcast);
        return new ChordMessage(JOIN_REQUEST, payload);
    }
    
    /**
     * Creates a message sent by the node that processes a join request
     * @param ft the accepting node's finger table, to initialize that of the new node
     * @return MessageObject of the correct type and payload
     */
    public static ChordMessage newJoinAcceptMessage(FingerTable ft, ChordID acceptedID) {
        Serializable[] payload = new Serializable[2];
        payload[0] = ft.toString();
        payload[1] = acceptedID;
        return new ChordMessage(JOIN_REPLY, payload);
    }
    
    /**
     * Creates a message sent periodically by nodes to query successors for any new predecessors, in
     * order to fix successor-predecessor links
     * @param retUri the return address for the reply
     * @return MessageObject of the correct type and payload
     */
    public static ChordMessage newStabilizeRequest(String retUri) {
        Serializable[] payload = new Serializable[1];
        payload[0] = retUri;
        return new ChordMessage(STABILIZE_REQUEST, payload);
    }
    
    /**
     * Creates a reply to a stabilize request
     * @param predecessor info of the queried node's predecessor
     * @return MessageObject of the correct type and payload
     */
    public static ChordMessage newStabilizeReply(ChordNodeInfo predecessor) {
        Serializable[] payload = new Serializable[1];
        payload[0] = predecessor;
        return new ChordMessage(STABILIZE_REPLY, payload);
    }
    
    /**
     * Creates a message sent by a node to notify a successor of its presence
     * @param predecessor info of the sender, possibly a new predecessor to the recipient
     * @return MessageObject of the correct type and payload
     */
    public static ChordMessage newNotifyMessage(ChordNodeInfo predecessor) {
        Serializable[] payload = new Serializable[1];
        payload[0] = predecessor;
        return new ChordMessage(NOTIFY, payload);
    }
    
    /**
     * Creates a message, similar to a notify message, sent by a node to inform a successor that
     * its current predecessor has possibly failed.
     * @param predecessor info of the sender, possibly a new predecessor to the recipient
     * @return MessageObject of the correct type and payload
     */
    public static ChordMessage newRepairMessage(ChordNodeInfo predecessor) {
        Serializable[] payload = new Serializable[1];
        payload[0] = predecessor;
        return new ChordMessage(REPAIR, payload);
    }
    
    /**
     * Creates a message sent by a node when asked to route a message on the network.
     * @param to final destination of the message
     * @param from contact info of the sender, in case a reply is needed
     * @param tag id of the application above the overlay that called the routing method
     * @param payload message contents
     * @return MessageObject of the correct type and payload
     */
    public static ChordMessage newRouteToMessage(ChordIDCluster to, ChordNodeInfo from, String tag, byte[] payload, boolean error, Vector clustersVisited) {
        Serializable[] pld = new Serializable[6];
        pld[0] = to;
        pld[1] = from;
        pld[2] = tag;
        pld[3] = payload;
        pld[4] = new Boolean(error);
        pld[5] = clustersVisited;
        return new ChordMessage(ROUTE, pld);
    }
    
    /**
     * Creates a message sent by a node when asked to resolve the given id
     * @param resolveID the id to resolve (lookup)
     * @param returnTo contact info of the node where the resolve request was made
     * @param failsafeMode set when a failure in the resolve path has caused a resolve request to timeout, so an alternative path must be found
     * @return MessageObject of the correct type and payload
     */
    public static ChordMessage newResolveRequest(ChordID resolveID, String returnTo, boolean failsafeMode) {
        Serializable[] payload = new Serializable[3];
        payload[0] = resolveID;
        payload[1] = returnTo;
        payload[2] = failsafeMode;
        return new ChordMessage(RESOLVE_REQUEST, payload);
    }
    
    /**
     * Creates a message sent by a node in response to a resolve request
     * @param nodeInfo contact info of the sender, which is the resolution of the resolve request
     * @return MessageObject of the correct type and payload
     */
    public static ChordMessage newResolveReply(ChordID resolveID, ChordNodeInfo nodeInfo) {
        Serializable[] payload = new Serializable[3];
        payload[0] = resolveID;
        payload[1] = nodeInfo.nodeID;
        payload[2] = nodeInfo.nodeURI;
        return new ChordMessage(RESOLVE_REPLY, payload);
    }
    
    /**
     * Creates a message sent by a node when leaving the network
     * @param leavingNode id of the sender
     * @param replacement contact info of the node that should replace the sender in the overlay
     * @return MessageObject of the correct type and payload
     */
    public static ChordMessage newLeaveMessage(ChordID leavingNode, ChordNodeInfo replacement) {
        Serializable[] payload = new Serializable[2];
        payload[0] = leavingNode;
        payload[1] = replacement;
        return new ChordMessage(LEAVING, payload);
    }
    
    public static ChordMessage newVirtualLeaveUpstreamMessage(ChordNodeInfo leavingNode, Vector<ChordNodeInfo> virtualChildren) {
        Serializable[] payload = new Serializable[2];
        payload[0] = leavingNode;
        payload[1] = virtualChildren;
        return new ChordMessage(VIRTUAL_LEAVE_UP, payload);
    }
    
    public static ChordMessage newVirtualLeaveDownstreamMessage(ChordNodeInfo leavingNode, ChordNodeInfo newParent) {
        Serializable[] payload;
        if (newParent != null) {
            payload = new Serializable[2];
            payload[0] = leavingNode;
            payload[1] = newParent;
        }
        else {
            payload = new Serializable[1];
            payload[0] = leavingNode;
        }
        return new ChordMessage(VIRTUAL_LEAVE_DOWN, payload);
    }
    
    /**
     * Creates a ping message used to check if a node is alive (in resolve failsafe mode)
     * @param replyTo the address of the node where the PING_ACK should be sent
     * @return MessageObject of the correct type and payload
     */
    public static ChordMessage newPingMessage(String replyTo) {
        Serializable[] payload = new Serializable[1];
        payload[0] = replyTo;
        return new ChordMessage(PING, payload);
    }
    
    /**
     * Creates a ping message used to check if a node is alive (in resolve failsafe mode)
     * @param replyTo the address of the node where the PING_ACK should be sent
     * @return MessageObject of the correct type and payload
     */
    public static ChordMessage newPingReply() {
        return new ChordMessage(PING_ACK, null);
    }
    
    /**
     *
     */
    public static ChordMessage newCreateVirtualNodeMessage(ChordID nodeID, ChordNodeInfo parentInfo, int depth) {
        Serializable[] payload = new Serializable[3];
        payload[0] = nodeID;
        payload[1] = parentInfo;
        payload[2] = depth;
        return new ChordMessage(CREATE_VIRTUAL, payload);
    }
    
    public static ChordMessage newAckVirtualNodeHost(ChordNodeInfo childInfo) {
        Serializable[] payload = new Serializable[1];
        payload[0] = childInfo;
        return new ChordMessage(ACK_VIRTUAL, payload);
    }

    /**
     * Creates an update message meant to update node's finger tables of possibly new nodes in the ring
     * @param nodeInfo the contact info for the new node
     * @return MessageObject of the correct type and payload
     */
    public static ChordMessage newNodeUpdateMessage(ChordNodeInfo nodeInfo, ChordID baseID) {
        Serializable[] payload = new Serializable[2];
        payload[0] = nodeInfo;
        payload[1] = baseID;
        return new ChordMessage(NODE_UPDATE, payload);
    }
    
    /**
     * Calls the appropriate message handling method on the local chord service reference according to
     * the current message type. Avoids having to use disparate getter methods for the different types
     * of payloads.
     * @param chordRef reference to the local chord overlay service
     */
    public void applyTo(ChordOverlayService chordRef) {
        switch (TYPE) {
            case JOIN_REQUEST: chordRef.processJoinRequest((ChordID) PAYLOAD[0], (String) PAYLOAD[1], ((Boolean) PAYLOAD[2]).booleanValue());
            break;
            case JOIN_REPLY: chordRef.completeJoin((String) PAYLOAD[0], (ChordID) PAYLOAD[1]);
            break;
            case STABILIZE_REQUEST: chordRef.processStabilizeRequest((String) PAYLOAD[0]);
            break;
            case STABILIZE_REPLY: chordRef.completeStabilize((ChordNodeInfo) PAYLOAD[0]);
            break;
            case NOTIFY: chordRef.processNotify((ChordNodeInfo) PAYLOAD[0]);
            break;
            case ROUTE:
                chordRef.relayRouteRequest((ChordIDCluster) PAYLOAD[0], (ChordNodeInfo) PAYLOAD[1], (String) PAYLOAD[2], (byte[]) PAYLOAD[3], ((Boolean) PAYLOAD[4]).booleanValue(), (Vector) PAYLOAD[5]);
                break;
            case RESOLVE_REQUEST: 
                chordRef.relayResolveRequest((ChordID) PAYLOAD[0], (String) PAYLOAD[1], (Boolean) PAYLOAD[2]);
            break;
            case RESOLVE_REPLY: chordRef.completeResolve((ChordID) PAYLOAD[0], new ChordNodeInfo((ChordID) PAYLOAD[1], (String) PAYLOAD[2]));
            break;
            case LEAVING: chordRef.processDeparture((ChordID) PAYLOAD[0], (ChordNodeInfo) PAYLOAD[1]);
            break;
            case REPAIR: chordRef.processRepair((ChordNodeInfo) PAYLOAD[0]);
            break;
            case PING: chordRef.processPing((String) PAYLOAD[0]);
            break;
            case PING_ACK: chordRef.completePing();
            break;
            case CREATE_VIRTUAL: chordRef.processCreateVirtual((ChordID) PAYLOAD[0], (ChordNodeInfo) PAYLOAD[1], (Integer) PAYLOAD[2]);
            break;
            case ACK_VIRTUAL: chordRef.completeCreateVirtual((ChordNodeInfo) PAYLOAD[0]);
            break;
            case VIRTUAL_LEAVE_UP: chordRef.processVirtualChildLeave((ChordNodeInfo) PAYLOAD[0], (Vector<ChordNodeInfo>) PAYLOAD[1]);
            break;
            case VIRTUAL_LEAVE_DOWN: 
                ChordNodeInfo newParent = null;
                if (PAYLOAD.length == 2) {
                    newParent = (ChordNodeInfo) PAYLOAD[1];
                }
                chordRef.processVirtualParentLeave((ChordNodeInfo) PAYLOAD[0], newParent);
            break;
            case NODE_UPDATE: chordRef.processNodeUpdate((ChordNodeInfo) PAYLOAD[0], (ChordID) PAYLOAD[1]);
            break;
        }
    }
    
}
