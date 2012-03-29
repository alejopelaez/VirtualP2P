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
 * OverlayProxy.java
 *
 * Created on November 1, 2008, 4:45 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package tassl.automate.overlay;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.Hashtable;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import programming5.concurrent.RequestVariable;
import programming5.net.MessageArrivedEvent;
import programming5.net.MessageArrivedListener;
import programming5.net.NetworkException;
import programming5.net.sockets.TCPClient;
import tassl.automate.overlay.replication.ReplicationAwareListener;
import tassl.automate.overlay.management.LoadManager;
import tassl.automate.util.MessageObject;

/**
 * An overlay proxy implements a generic overlay service interface and communicates over a network with an actual overlay 
 * service instance. The overlay proxy is instantiated by an application at a remote site, knowing a host address and port where 
 * an overlay server must be running.
 * @author Andres
 */
public class OverlayProxy implements OverlayService<OverlayID>, MessageArrivedListener {
    
    // Overlay objects
    protected OverlayID hostID;
    protected String hostURI;
    protected Hashtable<String, OverlayMessageListener> messageSubscribers = new Hashtable<String, OverlayMessageListener>();
    protected Vector<OverlayStructureListener> eventSubscribers = new Vector<OverlayStructureListener>();
    
    // Communication objects
    protected TCPClient overlayClient;
    
    // Condition and state variables
    protected Hashtable<Integer, RequestVariable> pendingRequests = new Hashtable<Integer, RequestVariable>();
    protected boolean joined = false;
    
    private long TIMEOUT = 10000;
    
    /** Creates a new instance of OverlayProxy */
    public OverlayProxy(String serverURI) {
        try {
            URI connectTo = new URI(serverURI);
            overlayClient = new TCPClient(connectTo.getHost(), connectTo.getPort());
            overlayClient.addListener(this);
            overlayClient.establishConnection();
        }
        catch (Exception e) {
            throw new RuntimeException("OverlayProxy: Could not connect to server: " + e.getMessage());
        }
    }

    public OverlayID join(String uri) throws IOException {
        if (!joined) {
            RequestVariable joinRequest = new RequestVariable();
            int requestID = putRequest(joinRequest, uri);
            sendMessage(OverlayProxyMessage.newJoinRequest(requestID, uri));
            waitFor(joinRequest);
            if (!joinRequest.isDone()) {
                throw new IOException("OverlayProxy: Join timed out");
            }
            else {
                Object result = joinRequest.getResult();
                if (!(result instanceof Exception)) {
                    processJoinReply((NodeInfo) result);
                }
                else {
                    throw new IOException(((Exception) result).getMessage());
                }
            }
        }
        return hostID;
    }

    public void leave() {
        if (joined) {
            RequestVariable leaveRequest = new RequestVariable();
            int requestID = putRequest(leaveRequest, new Random(System.currentTimeMillis()).nextInt());
            while (!leaveRequest.isDone()) {
                sendMessage(OverlayProxyMessage.newLeaveRequest(requestID));
                waitFor(leaveRequest);
            }
        }
    }

    public void terminate() {
        if (joined) {
            RequestVariable terminateRequest = new RequestVariable();
            int requestID = putRequest(terminateRequest, new Random(System.currentTimeMillis()).nextInt());
            while (!terminateRequest.isDone()) {
                sendMessage(OverlayProxyMessage.newTerminateRequest(requestID));
                waitFor(terminateRequest);
            }
        }
    }

    public OverlayID generateID(Object... parameters) {
        Serializable[] parameterArray = new Serializable[parameters.length];
        for (int i = 0; i < parameters.length; i++) {
            if (parameters[i] instanceof Serializable) {
                parameterArray[i] = (Serializable) parameters[i];
            }
            else {
                throw new UnsupportedOperationException("OverlayProxy: generateID operation unsupported for non-serializable parameters");
            }
        }
        RequestVariable idRequest = new RequestVariable();
        int requestID = putRequest(idRequest, parameters);
        while (!idRequest.isDone()) {
            sendMessage(OverlayProxyMessage.newGenerateIDRequest(requestID, parameterArray));
            waitFor(idRequest);
        }
        Object result = idRequest.getResult();
        OverlayID ret = null;
        if (!(result instanceof IllegalArgumentException)) {
            ret = (OverlayID) result;
        }
        else {
            throw (IllegalArgumentException) result;
        }
        return ret;
    }

    public void routeTo(OverlayID peer, String tag, byte[] payload) {
        Vector<OverlayID> auxVector = new Vector<OverlayID>();
        auxVector.add(peer);
        this.routeTo(auxVector, tag, payload);
    }

    public void routeTo(List<OverlayID> peers, String tag, byte[] payload) {
        if (joined) {
            try {
                overlayClient.send(OverlayProxyMessage.newRouteMessage((Vector<OverlayID>) peers, tag, payload).serialize());
            }
            catch (Exception e) {
                System.err.println("OverlayProxy: Cannot route message: " + e.getMessage());
            }
        }
        else {
            System.err.println("OverlayProxy: Cannot route message: Not joined");
        }
    }

    public void reliableRouteTo(OverlayID peer, String tag, byte[] payload) throws IOException {
        this.routeTo(peer, tag, payload);
    }

    public void reliableRouteTo(List<OverlayID> peers, String tag, byte[] payload) throws IOException {
        this.routeTo(peers, tag, payload);
    }

    public NodeInfo[] resolve(OverlayID peer) throws ResolveException {
        NodeInfo[] ret = null;
        if (joined) {
            RequestVariable resolveRequest = new RequestVariable();
            int requestID = putRequest(resolveRequest, peer);
            while (!resolveRequest.isDone()) {
                sendMessage(OverlayProxyMessage.newResolveRequest(requestID, peer));
                waitFor(resolveRequest);
            }
            Object result = resolveRequest.getResult();
            if (!(result instanceof ResolveException)) {
                ret = (NodeInfo[]) result;
            }
            else {
                throw (ResolveException) result;
            }
        }
        else {
            throw new RuntimeException("OverlayProxy: Cannot resolve ID: Not joined");
        }
        return ret;
    }

    public void sendDirect(NodeInfo node, String tag, byte[] payload) throws IOException {
        if (joined) {
            try {
                overlayClient.send(OverlayProxyMessage.newSendDirectMessage(node, tag, payload).serialize());
            }
            catch (Exception e) {
                throw new IOException("OverlayProxy: Could not send message: " + e.getMessage());
            }
        }
        else {
            throw new IOException("OverlayProxy: Cannot send message: Not joined");
        }
    }

    public NodeInfo getLocalID() {
        NodeInfo ret = null;
        if (joined) {
            ret = new NodeInfo();
            ret.nodeID = hostID;
            ret.nodeURI = hostURI;
        }
        return ret;
    }

    public NodeInfo[] getNeighborSet() {
        NodeInfo[] ret = null;
        if (joined) {
            RequestVariable neighborRequest = new RequestVariable();
            int requestID = putRequest(neighborRequest, new Random(System.currentTimeMillis()).nextInt());
            while (!neighborRequest.isDone()) {
                sendMessage(OverlayProxyMessage.newNeighborSetRequest(requestID));
                waitFor(neighborRequest);
            }
            ret = (NodeInfo[]) neighborRequest.getResult();
        }
        return ret;
    }

    public NodeNeighborhood getNeighborhood() {
        NodeNeighborhood ret = null;
        if (joined) {
            RequestVariable neighborRequest = new RequestVariable();
            int requestID = putRequest(neighborRequest, new Random(System.currentTimeMillis()).nextInt());
            while (!neighborRequest.isDone()) {
                sendMessage(OverlayProxyMessage.newNeighborhoodRequest(requestID));
                waitFor(neighborRequest);
            }
            ret = (NodeNeighborhood) neighborRequest.getResult();
        }
        return ret;
    }

    public void subscribe(OverlayListener listener, String tag) {
        boolean sendForMessages = (messageSubscribers.get(tag) == null);
        boolean sendForEvents = eventSubscribers.isEmpty();
        messageSubscribers.put(tag, listener);
        eventSubscribers.add(listener);
        if (sendForMessages && sendForEvents) {
            sendMessage(OverlayProxyMessage.newSubscribeRequest(tag));
        }
        else if (sendForMessages) {
            sendMessage(OverlayProxyMessage.newSubscribeMessagesRequest(tag));
        }
        else if (sendForEvents) {
            sendMessage(OverlayProxyMessage.newSubscribeEventsRequest());
        }
    }

    public void subscribe(ReplicationAwareListener listener, String tag, ApplicationState stateObject) {
        throw new java.lang.UnsupportedOperationException();
    }

    public void unsubscribe(OverlayListener listener, String tag) {
        messageSubscribers.remove(tag);
        sendMessage(OverlayProxyMessage.newUnsubscribeMessagesRequest(tag));
        eventSubscribers.remove(listener);
        if (eventSubscribers.isEmpty()) {
            sendMessage(OverlayProxyMessage.newUnsubscribeEventsRequest());
        }
    }

    public void subscribeToMessages(OverlayMessageListener listener, String tag) {
        boolean send = (messageSubscribers.get(tag) == null);
        messageSubscribers.put(tag, listener);
        if (send) {
            sendMessage(OverlayProxyMessage.newSubscribeMessagesRequest(tag));
        }
    }

    public void subscribeToMessages(ReplicationAwareListener listener, String tag, ApplicationState stateObject) {
        throw new UnsupportedOperationException();
    }

    public void unsubscribeFromMessages(OverlayMessageListener listener, String tag) {
        messageSubscribers.remove(tag);
        sendMessage(OverlayProxyMessage.newUnsubscribeMessagesRequest(tag));
    }

    public void subscribeToEvents(OverlayStructureListener listener) {
        boolean send = eventSubscribers.isEmpty();
        eventSubscribers.add(listener);
        if (send) {
            sendMessage(OverlayProxyMessage.newSubscribeEventsRequest());
        }
    }

    public void unsubscribeFromEvents(OverlayStructureListener listener) {
        eventSubscribers.remove(listener);
        if (eventSubscribers.isEmpty()) {
            sendMessage(OverlayProxyMessage.newUnsubscribeEventsRequest());
        }
    }

    public void updateApplicationState(String tag, ApplicationState stateObject) {
        throw new UnsupportedOperationException();
    }

    public void createVirtualNode(OverlayID nodeID) {
        if (joined) {
            sendMessage(OverlayProxyMessage.newCreateVirtualRequest(nodeID));
        }
    }

    public void attachLoadManager(LoadManager manager) {
    }

    public void signalEvent(MessageArrivedEvent event) {
        OverlayProxyMessage proxyMessage = (OverlayProxyMessage) MessageObject.createFromBytes(event.getMessageBytes());
        proxyMessage.applyTo(this);
    }
    
    protected void processJoinReply(NodeInfo hostNode) {
        hostID = hostNode.nodeID;
        hostURI = hostNode.nodeURI;
        joined = true;
    }
    
    protected void processJoinReply(int requestID, NodeInfo hostNode) {
        setResult(requestID, hostNode);
    }
    
    protected void completeLeave(int requestID) {
        for (OverlayStructureListener listener : eventSubscribers) {
            listener.leaving();
        }
        setResult(requestID, new Boolean(true));
    }

    protected void completeTerminate(int requestID) {
        for (OverlayStructureListener listener : eventSubscribers) {
            listener.terminating();
        }
        setResult(requestID, new Boolean(true));
    }
    
//    protected void processGenerateIDResult(int requestID, OverlayID result) {
//        setResult(requestID, result);
//    }
//    
//    protected void reportException(int requestID, Exception e) {
//        RequestVariable request = pendingRequests.get(requestID);
//        if (request != null) {
//            request.setResult(e);
//        }
//    }
    
//    private int getRequestID(Object... parameters) {
//        
//        return ret;
//    }
    
    protected synchronized void setResult(int requestID, Object result) {
        RequestVariable request = pendingRequests.get(requestID);
        if (request != null) {
            request.setResult(result);
            pendingRequests.remove(requestID);
        }
    }
    
    protected void processMessageCallback(MessageCallback callback) {
        final OverlayMessageListener listener = messageSubscribers.get(callback.getMessageTag());
        if (listener != null) {
            final MessageCallbackProxy callbackProxy = new MessageCallbackProxy(callback.getDestination(), callback.getMessageOrigin(), callback.getMessageTag(), callback.getMessageBytes());
            new Thread(new Runnable() {
                public void run() {
                    try {
                        listener.messageArrived(callbackProxy);
                    }
                    catch (Exception e) {
                        callbackProxy.serviceError(e.getMessage().getBytes());
                    }
                }
            }).start();
        }
    }
    
    protected void processNewNeighborCallback(int neighborType, NodeInfo newNode) {
        final int auxNeighborType = neighborType;
        final NodeInfo auxNewNode = newNode;
        for (final OverlayStructureListener listener : eventSubscribers) {
            new Thread(new Runnable() {
                public void run() {
                    listener.newNeighbor(auxNeighborType, auxNewNode);
                }
            }).start();
        }
    }
    
    protected void processNeighborDownCallback(int neighborType, NodeInfo[] failedNodes, NodeInfo[] replacements) {
        final int auxNeighborType = neighborType;
        final NodeInfo[] auxFailedNodes = failedNodes;
        final NodeInfo[] auxReplacements = replacements;
        for (final OverlayStructureListener listener : eventSubscribers) {
            new Thread(new Runnable() {
                public void run() {
                    listener.neighborsDown(auxNeighborType, auxFailedNodes, auxReplacements);
                }
            }).start();
        }
    }
    
    private synchronized int putRequest(RequestVariable request, Object... requestParameters) {
        int requestID = 1;
        for (Object parameter : requestParameters) {
            requestID *= parameter.hashCode();
        }
        while (pendingRequests.get(requestID) != null) {
            requestID *= requestID;
        }
        pendingRequests.put(requestID, request);
        return requestID;
    }
    
    private void sendMessage(OverlayProxyMessage message) {
        try {
            overlayClient.send(message.serialize());
        }
        catch (NetworkException ne) {
            ne.printStackTrace();
        }
    }
    
    private void waitFor(RequestVariable request) {
        try {
            request.await(TIMEOUT, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException ie) {
            ie.printStackTrace();
        }
    }
    
    protected class MessageCallbackProxy extends MessageCallback {
        
        public MessageCallbackProxy(Vector<OverlayID> to, NodeInfo from, String tag, byte[] payload) {
            super(to, from, tag, payload);
        }
        
        public void reply(byte[] replyMessage) {
            try {
                sendDirect(this.messageOrigin, this.messageTag, this.messageBytes);
            }
            catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }

        public void serviceError(byte[] errorMessage) {
//            overlayClient.send(OverlayProxyMessage.newCallbackError(errorMessage).serialize());
            // TODO: Complete functionality for service error
        }
        
    }
    
}
