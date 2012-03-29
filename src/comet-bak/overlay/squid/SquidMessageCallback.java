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

package tassl.automate.overlay.squid;

import java.io.IOException;
import java.util.Vector;
import programming5.io.Debug;
import tassl.automate.overlay.MessageCallback;
import tassl.automate.overlay.OverlayService;
import tassl.automate.overlay.chord.ChordNodeInfo;

public class SquidMessageCallback extends MessageCallback<SquidKey> {

    private static final long serialVersionUID = 5352634746074120355L;

    transient private OverlayService overlayRef;

    public SquidMessageCallback(SquidKey to, ChordNodeInfo from, String tag, byte[] userMessage, OverlayService myOverlay) {
        super(null, from, tag, userMessage);
        Vector<SquidKey> destVector = new Vector<SquidKey>();
        destVector.add(to);
        this.messageDestination = destVector;
        overlayRef = myOverlay;
    }

    public SquidKey getDestinationKey() {
        return this.messageDestination.elementAt(0);
    }

    // Sends a reply to the originator of the message
    @Override
    public void reply(byte[] replyMessage) {
        try {
            Debug.print("Sent reply at squid level ...","comet");
            overlayRef.sendDirect(this.messageOrigin, this.messageTag, replyMessage);
        }
        catch (IOException ioe) {
            throw new RuntimeException("SquidOverlayService: Cannot send reply: " + ioe.getMessage());
        }
    }

    // Sends an error as a reply to the originator of the message, unless it is possible to relay
    // the message to another cluster.
    @Override
    public void serviceError(byte[] errorMessage) {
    }

}
