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
 * OverlayProxyMessage.java
 *
 * Created on November 1, 2008, 4:47 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package tassl.automate.overlay;

import java.io.Serializable;
import java.util.Vector;
import tassl.automate.util.MessageObject;

/**
 * Message object used by the OverlayProxy and OverlayProxy server classes
 * @author Andres
 */
public class OverlayProxyMessage extends MessageObject {
    
    private static final long serialVersionUID = 4417322907130828816L;
    
    public static final int EXCEPTION = 0;
    public static final int JOIN_REQUEST = 1;
    public static final int JOIN_REPLY = -1;
    public static final int LEAVE_REQUEST = 2;
    public static final int LEAVE_COMPLETE = -2;
    public static final int GENERATE_ID_REQUEST = 3;
    public static final int GENERATE_ID_REPLY = -3;
    public static final int ROUTE_MESSAGE = 4;
    public static final int RESOLVE_REQUEST = 5;
    public static final int RESOLVE_REPLY = -5;
    public static final int SEND_MESSAGE = 6;
    public static final int CREATEVIRTUAL_REQUEST = 7;
    public static final int NEIGHBORSET_REQUEST = 8;
    public static final int NEIGHBORSET_REPLY = -8;
    public static final int NEIGHBORHOOD_REQUEST = 9;
    public static final int NEIGHBORHOOD_REPLY = -9;
    public static final int SUBSCRIBE = 10;
    public static final int SUBSCRIBE_MESSAGES = 11;
    public static final int SUBSCRIBE_EVENTS = 12;
    public static final int REPLICATE_FULL_REQUEST = 13;
    public static final int REPLICATE_PART_REQUEST = 14;
    public static final int UNSUBSCRIBE_MESSAGES = 15;
    public static final int UNSUBSCRIBE_EVENTS = 16;
    public static final int MESSAGE_CALLBACK = -17;
    public static final int CALLBACK_ERROR = -18;
    public static final int NEWNEIGHBOR_CALLBACK = -19;
    public static final int NEIGHBORDOWN_CALLBACK = -20;
    public static final int TERMINATE_REQUEST = 21;
    public static final int TERMINATE_COMPLETE = -21;
    
    /** Creates a new instance of OverlayProxyMessage */
    private OverlayProxyMessage(int type, Serializable... payload) {
        super(type, payload);
    }
    
    public static OverlayProxyMessage newExceptionMessage(int requestID, Exception e) {
        return new OverlayProxyMessage(EXCEPTION, requestID, e);
    }
    
    public static OverlayProxyMessage newJoinRequest(int requestID, String uri) {
        return new OverlayProxyMessage(JOIN_REQUEST, requestID, uri);
    }
    
    public static OverlayProxyMessage newJoinReply(int requestID, NodeInfo node) {
        return new OverlayProxyMessage(JOIN_REPLY, requestID, node);
    }
    
    public static OverlayProxyMessage newLeaveRequest(int requestID) {
        return new OverlayProxyMessage(LEAVE_REQUEST, requestID);
    }
    
    public static OverlayProxyMessage newLeaveCompleteMessage(int requestID) {
        return new OverlayProxyMessage(LEAVE_COMPLETE, requestID);
    }
    
    public static OverlayProxyMessage newGenerateIDRequest(int requestID, Serializable[] requestParams) {
        return new OverlayProxyMessage(GENERATE_ID_REQUEST, requestID, requestParams);
    }
    
    public static OverlayProxyMessage newGenerateIDReply(int requestID, OverlayID reply) {
        return new OverlayProxyMessage(GENERATE_ID_REPLY, requestID, reply);
    }
    
    public static OverlayProxyMessage newRouteMessage(Vector<OverlayID> peers, String tag, byte[] payload) {
        return new OverlayProxyMessage(ROUTE_MESSAGE, peers, tag, payload);
    }
    
    public static OverlayProxyMessage newResolveRequest(int requestID, OverlayID peer) {
        return new OverlayProxyMessage(RESOLVE_REQUEST, requestID, peer);
    }
    
    public static OverlayProxyMessage newResolveReply(int requestID, NodeInfo[] result) {
        return new OverlayProxyMessage(RESOLVE_REPLY, requestID, result);
    }
    
    public static OverlayProxyMessage newSendDirectMessage(NodeInfo destination, String tag, byte[] payload) {
        return new OverlayProxyMessage(SEND_MESSAGE, destination, tag, payload);
    }
    
    public static OverlayProxyMessage newNeighborSetRequest(int requestID) {
        return new OverlayProxyMessage(NEIGHBORSET_REQUEST, requestID);
    }
    
    public static OverlayProxyMessage newNeighborSetReply(int requestID, NodeInfo[] reply) {
        return new OverlayProxyMessage(NEIGHBORSET_REPLY, requestID, reply);
    }
    
    public static OverlayProxyMessage newNeighborhoodRequest(int requestID) {
        return new OverlayProxyMessage(NEIGHBORHOOD_REQUEST, requestID);
    }
    
    public static OverlayProxyMessage newNeighborhoodReply(int requestID, NodeNeighborhood reply) {
        return new OverlayProxyMessage(NEIGHBORHOOD_REPLY, requestID, reply);
    }
    
    public static OverlayProxyMessage newSubscribeRequest(String tag) {
        return new OverlayProxyMessage(SUBSCRIBE, tag);
    }
    
    public static OverlayProxyMessage newSubscribeMessagesRequest(String tag) {
        return new OverlayProxyMessage(SUBSCRIBE_MESSAGES, tag);
    }
    
    public static OverlayProxyMessage newSubscribeEventsRequest() {
        return new OverlayProxyMessage(SUBSCRIBE_EVENTS);
    }
    
    public static OverlayProxyMessage newMessageCallback(MessageCallback callback) {
        return new OverlayProxyMessage(MESSAGE_CALLBACK, callback);
    }
    
    public static OverlayProxyMessage newCallbackError(byte[] errorMessage) {
        return new OverlayProxyMessage(CALLBACK_ERROR, errorMessage);
    }
    
    public static OverlayProxyMessage newCreateVirtualRequest(OverlayID nodeID) {
        return new OverlayProxyMessage(CREATEVIRTUAL_REQUEST, nodeID);
    }
    
    public static OverlayProxyMessage newNewNeighborCallback(int neighborType, NodeInfo newNode) {
        return new OverlayProxyMessage(NEWNEIGHBOR_CALLBACK, neighborType, newNode);
    }
    
    public static OverlayProxyMessage newNeighborDownCallback(int neighborType, NodeInfo[] failedNodes, NodeInfo[] replacements) {
        return new OverlayProxyMessage(NEIGHBORDOWN_CALLBACK, neighborType, failedNodes, replacements);
    }
    
    public static OverlayProxyMessage newUnsubscribeMessagesRequest(String tag) {
        return new OverlayProxyMessage(UNSUBSCRIBE_MESSAGES, tag);
    }
    
    public static OverlayProxyMessage newUnsubscribeEventsRequest() {
        return new OverlayProxyMessage(UNSUBSCRIBE_EVENTS);
    }
    
    public static OverlayProxyMessage newTerminateRequest(int requestID) {
        return new OverlayProxyMessage(TERMINATE_REQUEST, requestID);
    }

    public static OverlayProxyMessage newTerminateCompleteMessage(int requestID) {
        return new OverlayProxyMessage(TERMINATE_COMPLETE, requestID);
    }

    public void applyTo(OverlayProxyServer proxyServer) {
        switch (this.TYPE) {
            case JOIN_REQUEST: proxyServer.processJoinRequest((Integer) PAYLOAD[0], (String) PAYLOAD[1]);
            break;
            case LEAVE_REQUEST: proxyServer.processLeaveRequest((Integer) PAYLOAD[0]);
            break;
            case GENERATE_ID_REQUEST: proxyServer.processGenerateIDRequest((Integer) PAYLOAD[0], (Object[]) PAYLOAD[1]);
            break;
            case NEIGHBORSET_REQUEST: proxyServer.processNeighborSetRequest((Integer) PAYLOAD[0]);
            break;
            case NEIGHBORHOOD_REQUEST: proxyServer.processNeighborhoodRequest((Integer) PAYLOAD[0]);
            break;
            case ROUTE_MESSAGE: proxyServer.processRouteRequest((Vector<OverlayID>) PAYLOAD[0], (String) PAYLOAD[1], (byte[]) PAYLOAD[2]);
            break;
            case RESOLVE_REQUEST: proxyServer.processResolveRequest((Integer) PAYLOAD[0], (OverlayID) PAYLOAD[1]);
            break;
            case SEND_MESSAGE: proxyServer.processSendDirectMessage((NodeInfo) PAYLOAD[0], (String) PAYLOAD[1], (byte[]) PAYLOAD[2]);
            break;
            case SUBSCRIBE: proxyServer.processSubscribeRequest((String) PAYLOAD[0]);
            break;
            case SUBSCRIBE_MESSAGES: proxyServer.processSubscribeMessagesRequest((String) PAYLOAD[0]);
            break;
            case SUBSCRIBE_EVENTS: proxyServer.processSubscribeEventsRequest();
            break;
            case UNSUBSCRIBE_MESSAGES: proxyServer.processUnsubscribeMessagesRequest((String) PAYLOAD[0]);
            break;
            case UNSUBSCRIBE_EVENTS: proxyServer.processUnsubscribeEventsRequest();
            break;
            case CREATEVIRTUAL_REQUEST: proxyServer.processCreateVirtualRequest((OverlayID) PAYLOAD[0]);
            break;
            case TERMINATE_REQUEST: proxyServer.processTerminateRequest((Integer) PAYLOAD[0]);
            break;
        }
    }
    
    public void applyTo(OverlayProxy proxy) {
        switch (this.TYPE) {
            case JOIN_REPLY: 
                int requestType = (Integer) PAYLOAD[0];
                if (requestType == 0) {
                    proxy.processJoinReply((NodeInfo) PAYLOAD[1]);
                }
                else {
                    proxy.processJoinReply(requestType, (NodeInfo) PAYLOAD[1]);
                }
            break;
            case LEAVE_COMPLETE: proxy.completeLeave((Integer) PAYLOAD[0]);
            break;
            case TERMINATE_COMPLETE: proxy.completeTerminate((Integer) PAYLOAD[0]);
            break;
            case MESSAGE_CALLBACK: proxy.processMessageCallback((MessageCallback) PAYLOAD[0]);
            break;
            case NEWNEIGHBOR_CALLBACK: proxy.processNewNeighborCallback((Integer) PAYLOAD[0], (NodeInfo) PAYLOAD[1]);
            break;
            case NEIGHBORDOWN_CALLBACK: proxy.processNeighborDownCallback((Integer) PAYLOAD[0], (NodeInfo[]) PAYLOAD[1], (NodeInfo[]) PAYLOAD[2]);
            break;
            default: proxy.setResult((Integer) PAYLOAD[0], PAYLOAD[1]);
        }
    }
    
}
