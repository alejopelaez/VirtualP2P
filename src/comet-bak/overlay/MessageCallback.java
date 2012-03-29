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

import java.io.Serializable;
import java.util.Vector;

/**
 * This class is meant to encapsulate both message parameters for the benefit of a receiving 
 * OverlayListener and the logic to get a reply or error to the sender. Get methods are provided for the 
 * former, but the latter depends on the overlay implementation, because of which the definition of the 
 * class is abstract. 
 * @author ahernandez
 */
public abstract class MessageCallback<T extends OverlayID> implements Serializable {

    private static final long serialVersionUID = 9053287211204712857L;
    
    protected NodeInfo messageOrigin = null;
    protected Vector<T> messageDestination = null;
    protected String messageTag = null;
    protected byte[] messageBytes = null;

    public MessageCallback(Vector<T> to, NodeInfo from, String tag, byte[] payload) {
        messageDestination = to;
        messageOrigin = from;
        messageTag = tag;
        messageBytes = payload;
    }

    public byte[] getMessageBytes() {
        return messageBytes;
    }

    public String getMessageTag() {
        return messageTag;
    }

    public NodeInfo getMessageOrigin() {
        NodeInfo ret = new NodeInfo();
        ret.nodeID = messageOrigin.nodeID;
        ret.nodeURI = messageOrigin.nodeURI;
        return ret;
    }

    public Vector<T> getDestination() {
        return messageDestination;
    }

    /**
     * A listener can use this method to reply to a receive message without handling destination
     * information.
     * @param replyMessage the application-specific reply
     */
    public abstract void reply(byte[] replyMessage);

    /**
     * A listener can use this method to notify the sender of errors triggered by the received message.
     * @param errorMessage application-specific error
     */
    public abstract void serviceError(byte[] errorMessage);

}
