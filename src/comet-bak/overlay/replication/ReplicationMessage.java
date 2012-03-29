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
 * ReplicationMessage.java
 *
 * Created on September 9, 2008, 3:01 PM
 */

package tassl.automate.overlay.replication;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;
import tassl.automate.overlay.ApplicationState;
import tassl.automate.overlay.MessageCallback;
import tassl.automate.util.MessageObject;

/**
 * Message object definition for the replication layer
 * @author aquirozh
 */
public class ReplicationMessage extends MessageObject {
    
    private static final long serialVersionUID = 2685593467334667659L;
                                        
    // Message codes
    private static final int EVENT = 1;
    private static final int STATE = 2;
    private static final int RECOVER = 3;
    private static final int UPDATE = 4;
    private static final int INSERT = 5;
    private static final int BACKWARD = 6;
    private static final int LEAVING = 7;
    
    private ReplicationMessage(int type, Serializable[] payload) {
        super(type, payload);
    }
    
    public static ReplicationMessage newEventMessage(MessageCallback c, int prevReplications) {
        Serializable[] payload = new Serializable[2];
        payload[0] = prevReplications;
        payload[1] = c;
        return new ReplicationMessage(EVENT, payload);
    }
    
    public static ReplicationMessage newStateMessage(String tag, ApplicationState stateObject, int prevReplications) {
        Serializable[] payload = new Serializable[3];
        payload[0] = prevReplications;
        payload[1] = tag;
        payload[2] = stateObject;
        return new ReplicationMessage(STATE, payload);
    }
    
    public static ReplicationMessage newRecoverMessage(int distance, Hashtable<String, ApplicationState> stateTable) {
        Serializable[] payload = new Serializable[2];
        payload[0] = distance;
        payload[1] = stateTable;
        return new ReplicationMessage(RECOVER, payload);
    }
    
    public static ReplicationMessage newLeavingMessage(int distance, Hashtable<String, ApplicationState> recoveryTable, Hashtable<String, ApplicationState> replacementTable) {
        Serializable[] payload = new Serializable[3];
        payload[0] = distance;
        payload[1] = recoveryTable;
        payload[2] = replacementTable;
        return new ReplicationMessage(LEAVING, payload);
    }
    
    public static ReplicationMessage newUpdateMessage(Vector<Hashtable<String, ApplicationState>> stateStore/*, Vector<Hashtable<String, Queue<MessageCallback>>> eventStore*/) {
        Serializable[] payload = new Serializable[1];
        payload[0] = stateStore;
        return new ReplicationMessage(UPDATE, payload);
    }
    
    public static ReplicationMessage newInsertMessage(int distance) {
        Serializable[] payload = new Serializable[1];
        payload[0] = distance;
        return new ReplicationMessage(INSERT, payload);
    }
    
    public static ReplicationMessage newBackwardMessage(Hashtable<String, ApplicationState> stateTable) {
        Serializable[] payload = new Serializable[1];
        payload[0] = stateTable;
        return new ReplicationMessage(BACKWARD, payload);
    }
    
    public void applyTo(ReplicationLayer storeRef) {
        switch (TYPE) {
            case EVENT: storeRef.doReplicateEvent((MessageCallback) PAYLOAD[1], (Integer) PAYLOAD[0]);
            break;
            case STATE: storeRef.doReplicateState((String) PAYLOAD[1], (ApplicationState) PAYLOAD[2], (Integer) PAYLOAD[0]);
            break;
            case RECOVER: storeRef.signalRecover((Integer) PAYLOAD[0], (Hashtable<String, ApplicationState>) PAYLOAD[1]);
            break;
            case UPDATE: storeRef.doUpdate((Vector<Hashtable<String, ApplicationState>>) PAYLOAD[0]/*, (Vector<Hashtable<String, Queue<MessageCallback>>>) PAYLOAD[1]*/);
            break;
            case INSERT: storeRef.doInsert((Integer) PAYLOAD[0]);
            break;
            case BACKWARD: storeRef.doBackwardAccept((Hashtable<String, ApplicationState>) PAYLOAD[0]);
            break;
            case LEAVING: storeRef.handleLeaving((Integer) PAYLOAD[0], (Hashtable<String, ApplicationState>) PAYLOAD[1], (Hashtable<String, ApplicationState>) PAYLOAD[2]);
            break;
        }
    }
    
}
