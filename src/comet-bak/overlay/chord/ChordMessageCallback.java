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

import java.util.Vector;
import tassl.automate.overlay.MessageCallback;

/**
 * Chord implementation of the overlay.MessageCallback object, which uses ChordIDs.
 * @author aquirozh
 */
public class ChordMessageCallback extends MessageCallback<ChordID> {
        
        private static final long serialVersionUID = 8382474696320401104L;
        
        Vector clusterHistory;
        
        transient private ChordOverlayService overlayRef;
        
        public ChordMessageCallback(ChordIDCluster to, ChordNodeInfo from, String tag, byte[] payload, Vector clustersVisited, ChordOverlayService myOverlay) {
            super(to, from, tag, payload);
            clusterHistory = clustersVisited;
            overlayRef = myOverlay;
        }
        
        // Sends a reply to the originator of the message
        public void reply(byte[] replyMessage) {
            try {
                ChordIDCluster origin = new ChordIDCluster(((ChordID) overlayRef.getLocalID().nodeID).NUM_BITS);
                origin.add((ChordID) this.messageOrigin.nodeID);
                byte[] message = ChordMessage.newRouteToMessage(origin,
                        (ChordNodeInfo) overlayRef.getLocalID(),
                        this.messageTag,
                        replyMessage,
                        false,
                        new Vector()).serialize();
                overlayRef.network.send(message, this.messageOrigin.nodeURI);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
        
        // Sends an error as a reply to the originator of the message, unless it is possible to relay
        // the message to another cluster.
        public void serviceError(byte[] errorMessage) {
            try {
                String destination = null;
                byte[] message = null;
                if (((ChordID) this.messageDestination.get(0)).isOfAnyCluster()) {
                    ChordNodeInfo[] neighbors = ((ChordNodeNeighborhood) overlayRef.getNeighborhood()).getRemoteSuccessors();
                    for (ChordNodeInfo neighbor : neighbors) {
                        String nextCluster = neighbor.getChordID().getCluster();
                        if (!clusterHistory.contains(nextCluster)) {
                            clusterHistory.add(((ChordID) overlayRef.getLocalID().nodeID).getCluster());
                            message = ChordMessage.newRouteToMessage((ChordIDCluster) this.messageDestination,
                                    (ChordNodeInfo) this.messageOrigin,
                                    this.messageTag,
                                    this.messageBytes,
                                    false,
                                    clusterHistory).serialize();
                            destination = neighbor.nodeURI;
                            break;
                        }
                    }
                }
                if (destination == null) {
                    ChordIDCluster origin = new ChordIDCluster(((ChordID) overlayRef.getLocalID().nodeID).NUM_BITS);
                    origin.add((ChordID) this.messageOrigin.nodeID);
                    message = ChordMessage.newRouteToMessage(origin,
                                (ChordNodeInfo) overlayRef.getLocalID(),
                                    this.messageTag,
                                    errorMessage,
                                    true,
                                    new Vector()).serialize();
                    destination = this.messageOrigin.nodeURI;
                }
                overlayRef.network.send(message, destination);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
        
    }
