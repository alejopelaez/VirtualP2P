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
 * OverlayProxyServer.java
 *
 * Created on November 1, 2008, 6:28 PM
 */

package tassl.automate.overlay;

import java.io.IOException;
import java.util.Vector;
import programming5.net.MessageArrivedEvent;
import programming5.net.MessageArrivedListener;
import programming5.net.MessagingClient;
import programming5.net.NetworkException;
import programming5.net.PluggableClient;
import programming5.net.ServiceObject;

/**
 * Server side implementation of an overlay proxy, that interacts directly with the overlay service object on behalf of the remote 
 * client. One OverlayProxyServer object must be created for each remote client (overlay proxy instance); an OverlayProxyDaemon thread 
 * is provided for this task.
 * @author Andres
 */
public class OverlayProxyServer implements ServiceObject, MessageArrivedListener, OverlayListener {
    
    MessagingClient client;
    OverlayService overlay;
    
    /** Creates a new instance of OverlayProxyServer */
    public OverlayProxyServer(OverlayService overlayRef) {
        overlay = overlayRef;
    }

    public void newClient(PluggableClient c) {
        client = (MessagingClient) c;
        client.addListener(this);
        NodeInfo nodeInfo = overlay.getLocalID();
        if (nodeInfo != null) {
            sendMessage(OverlayProxyMessage.newJoinReply(0, nodeInfo));
        }
    }

    public void messageArrived(MessageCallback c) {
        sendMessage(OverlayProxyMessage.newMessageCallback(c));
    }

    public void serviceErrorOcurred(NodeInfo where, byte[] errorMessage) {
    }

    public OverlayMessageListener createNew(OverlayService serviceRef) {
        return null;
    }

    public void leaving() {
    }

    public void terminating() {
    }

    public void newNeighbor(NodeInfo newNode) {
    }

    public void newNeighbor(int neighborType, NodeInfo newNode) {
        sendMessage(OverlayProxyMessage.newNewNeighborCallback(neighborType, newNode));
    }

    public void neighborDown(OverlayID absentNode) {
    }

    public void neighborsDown(int neighborType, NodeInfo[] failedNodes, NodeInfo[] replacements) {
        sendMessage(OverlayProxyMessage.newNeighborDownCallback(neighborType, failedNodes, replacements));
    }

    public void signalEvent(MessageArrivedEvent event) {
        OverlayProxyMessage proxyMessage = (OverlayProxyMessage) OverlayProxyMessage.createFromBytes(event.getMessageBytes());
        proxyMessage.applyTo(this);
    }
    
    protected void processJoinRequest(int requestID, String uri) {
        try {
            overlay.join(uri);
            NodeInfo ret = overlay.getLocalID();
            if (ret != null) {
                sendMessage(OverlayProxyMessage.newJoinReply(requestID, ret));
            }
        }
        catch (IOException ioe) {
            sendMessage(OverlayProxyMessage.newExceptionMessage(requestID, ioe));
        }
    }
    
    protected void processLeaveRequest(int requestID) {
        overlay.leave();
        sendMessage(OverlayProxyMessage.newLeaveCompleteMessage(requestID));
    }
    
    protected void processTerminateRequest(int requestID) {
        overlay.terminate();
        sendMessage(OverlayProxyMessage.newTerminateCompleteMessage(requestID));
    }

    protected void processGenerateIDRequest(int requestID, Object[] parameters) {
        try {
            OverlayID ret = overlay.generateID(parameters);
            if (ret != null) {
                sendMessage(OverlayProxyMessage.newGenerateIDReply(requestID, ret));
            }
        }
        catch (IllegalArgumentException iae) {
            sendMessage(OverlayProxyMessage.newExceptionMessage(requestID, iae));
        }
    }
    
    protected void processRouteRequest(Vector<OverlayID> peers, String tag, byte[] payload) {
        overlay.routeTo(peers, tag, payload);
    }
    
    protected void processResolveRequest(int requestID, OverlayID peer) {
        try {
            NodeInfo[] ret = overlay.resolve(peer);
            if (ret != null) {
                sendMessage(OverlayProxyMessage.newResolveReply(requestID, ret));
            }
        }
        catch (ResolveException re) {
            sendMessage(OverlayProxyMessage.newExceptionMessage(requestID, re));
        }
    }
    
    protected void processSendDirectMessage(NodeInfo destination, String tag, byte[] payload) {
        try {
            overlay.sendDirect(destination, tag, payload);
        }
        catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
    
    protected void processNeighborSetRequest(int requestID) {
        NodeInfo[] ret = overlay.getNeighborSet();
        sendMessage(OverlayProxyMessage.newNeighborSetReply(requestID, ret));
    }
    
    protected void processNeighborhoodRequest(int requestID) {
        NodeNeighborhood ret = overlay.getNeighborhood();
        sendMessage(OverlayProxyMessage.newNeighborhoodReply(requestID, ret));
    }
    
    protected void processSubscribeRequest(String tag) {
        overlay.subscribe(this, tag);
    }
    
    protected void processSubscribeMessagesRequest(String tag) {
        overlay.subscribeToMessages(this, tag);
    }
    
    protected void processSubscribeEventsRequest() {
        overlay.subscribeToEvents(this);
    }
    
    protected void processUnsubscribeMessagesRequest(String tag) {
        overlay.unsubscribeFromMessages(this, tag);
    }
    
    protected void processUnsubscribeEventsRequest() {
        overlay.unsubscribeFromEvents(this);
    }
    
    protected void processCreateVirtualRequest(OverlayID nodeID) {
        overlay.createVirtualNode(nodeID);
    }
    
    private void sendMessage(OverlayProxyMessage message) {
        try {
            client.send(message.serialize());
        }
        catch (NetworkException ne) {
            ne.printStackTrace();
        }
    }
    
}
