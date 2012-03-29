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
 * SquidMessage.java
 *
 * Created on August 23, 2007, 3:11 PM
 */

package tassl.automate.overlay.squid;

import java.io.Serializable;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Vector;
import tassl.automate.overlay.NodeInfo;
import tassl.automate.overlay.chord.ChordNodeInfo;
import tassl.automate.util.MessageObject;

/**
 *
 * @author aquirozh
 */
public class SquidMessage extends MessageObject {

    private static final long serialVersionUID = 4331658867540438663L;

    // Message codes
    private static final int USER_MSG = 1;
    private static final int ROUTE_MSG = 2;
    private static final int RESOLVE_REQ = 3;
    private static final int RESOLVE_RESP = -3;
    
    /*
     * Hidden constructor to call the MessageObject constructor with message codes as types and specific 
     * payloads
     */
    private SquidMessage(int type, Serializable[] payload) {
        super(type, payload);
    }
    
    /**
     * Creates a message sent by a node when asked to route a message by the user.
     * @param tag id of the application above the overlay that called the routing method
     * @param payload message contents
     * @return MessageObject of the correct type and payload
     */
    public static SquidMessage newUserMessage(SquidKey destination, ChordNodeInfo origin, String tag, byte[] payload) {
        Serializable[] pld = new Serializable[5];
        pld[0] = origin;
        pld[1] = tag;
        pld[2] = payload;
        pld[3] = System.currentTimeMillis();
        pld[4] = destination;
        return new SquidMessage(USER_MSG, pld);
    }
    
    public static SquidMessage newRoutingMessage(BigInteger[] range, int refinement, BigInteger partialIndex, SquidMessage userMessage) {
        Serializable[] pld = new Serializable[range.length + 4];
        pld[0] = new Integer(range.length);
        int i = 0;
        for (; i < range.length; i++) {
            pld[i+1] = range[i];
        }
        pld[++i] = refinement;
        pld[++i] = partialIndex;
        pld[++i] = userMessage;
        return new SquidMessage(ROUTE_MSG, pld);
    }
    
    public static SquidMessage newResolveRequest(int resolveKey, BigInteger callKey, ChordNodeInfo origin) {
        Serializable[] pld = new Serializable[3];
        pld[0] = origin;
        pld[1] = resolveKey;
        pld[2] = callKey;
        return new SquidMessage(RESOLVE_REQ, pld);
    }
    
    public static SquidMessage newResolveReply(int resolveKey, BigInteger callKey, Vector<NodeInfo> response) {
        Serializable[] pld = new Serializable[3];
        pld[0] = resolveKey;
        pld[1] = response;
        pld[2] = callKey;
        return new SquidMessage(RESOLVE_RESP, pld);
    }
    
    /**
     * Calls the appropriate message handling method on the local squid service reference according to 
     * the current message type. Avoids having to use disparate getter methods for the different types 
     * of payloads.
     * @param squidRef reference to the local squid overlay service
     */
    public void applyTo(SquidOverlayService squidRef) {
        switch (TYPE) {
            case ROUTE_MSG: 
                BigInteger[] range = new BigInteger[(Integer) PAYLOAD[0]];
                int i = 0;
                for (; i < range.length; i++) {
                    range[i] = (BigInteger) PAYLOAD[i+1];
                }
                if (((SquidMessage) PAYLOAD[i+3]).isUserMsg()) {
                    squidRef.processRouteRequest(range, (Integer) PAYLOAD[i+1], (BigInteger) PAYLOAD[i+2], (SquidMessage) PAYLOAD[i+3]);
                }
                else if (((SquidMessage) PAYLOAD[i+3]).TYPE == RESOLVE_REQ) {
                    squidRef.processResolveRequest(range, (Integer) PAYLOAD[i+1], (BigInteger) PAYLOAD[i+2], (SquidMessage) PAYLOAD[i+3]);
                }
                break;
            case RESOLVE_RESP:
                squidRef.processResolveReply((Integer) PAYLOAD[0], (BigInteger) PAYLOAD[2], (Vector<NodeInfo>) PAYLOAD[1]);
                break;
        }
    }
    
    public boolean isUserMsg() {
        return this.TYPE == USER_MSG;
    }
    
    public SquidKey getDestinationKey() {
        SquidKey ret = null;
        if (this.isUserMsg()) {
            ret = (SquidKey) PAYLOAD[4];
        }
        return ret;
    }

    public ChordNodeInfo getMsgOrigin() {
        ChordNodeInfo ret = (ChordNodeInfo) PAYLOAD[0];
        return ret;
    }
    
    public String getUserMsgTag() {
        String ret = null;
        if (this.isUserMsg()) {
            ret = (String) PAYLOAD[1];
        }
        return ret;
    }
    
    public byte[] getUserMsgBytes() {
        byte[] ret = null;
        if (this.isUserMsg()) {
            ret = (byte[]) PAYLOAD[2];
        }
        return ret;
    }
    
    public int getResolveKey() {
        int ret = -1;
        if (this.TYPE == RESOLVE_REQ) {
            ret = (Integer) PAYLOAD[1];
        }
        return ret;
    }
    
    public BigInteger getCallKey() {
        BigInteger ret = null;
        if (this.TYPE == RESOLVE_REQ) {
            ret = (BigInteger) PAYLOAD[2];
        }
        return ret;
    }
    
    public byte[] getMessageHash() {
        byte[] ret = null;
        try {
            MessageDigest hash = MessageDigest.getInstance("MD5");
            for (Object item : PAYLOAD) {
                byte[] itemBytes;
                try {
                    itemBytes = (byte[]) item;
                }
                catch (ClassCastException cce) {
                    itemBytes = item.toString().getBytes();
                }
                hash.update(itemBytes);
            }
            ret = hash.digest();
        }
        catch (NoSuchAlgorithmException nsa) {
            nsa.printStackTrace();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            return ret;
        }
    }
    
}
