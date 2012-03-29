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
import programming5.net.Message;

/**
 * Contact and (possibly) other information of an overlay node that can be kept and transmitted over the 
 * network. 
 * @author ahernandez
 */
public class NodeInfo implements Serializable {

    private static final long serialVersionUID = 6378560288137694280L;
    /**
     * Identifier of the node in the overlay ID space
     */
    public OverlayID nodeID;
    /**
     * Physical address of the node in the underlying network infrastructure
     */
    public String nodeURI;

    @Override
    public String toString() {
        String ret = null;
        String idString = (nodeID != null) ? nodeID.toString() : "EmptyID";
        String uriString = (nodeURI != null) ? nodeURI : "EmptyURI";
        return new String(Message.constructHeaderlessMessage(idString, uriString).getMessageBytes());
    }

    @Override
    public boolean equals(Object other) {
        boolean ret = false;
        if (other instanceof NodeInfo) {
            NodeInfo otherNode = (NodeInfo) other;
            ret = this.nodeURI.equals(otherNode.nodeURI) && this.nodeID.equals(otherNode.nodeID);
        }
        return ret;
    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }
}
