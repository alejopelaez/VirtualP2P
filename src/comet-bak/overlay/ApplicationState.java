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
 * ApplicationState.java
 *
 * Created on September 10, 2008, 2:52 PM
 */

package tassl.automate.overlay;

import java.io.Serializable;

/**
 * An application state object represents a data handling object at the application level, updated through 
 * the messages delivered to the local node. For replication, the implementing object should be a copy 
 * of the actual state of the application (otherwise, double updates can occur), and should not perform 
 * any external actions that would otherwise be performed by the application upon the reception of message events.
 * @author aquirozh
 */
public interface ApplicationState extends Serializable {
    
    /**
     * Called when the current node must assume the application state (data) of another node, because of the latter's departure or failure
     */
    public void merge(ApplicationState state);
    
    /**
     * Called when another (new) node should assume part of the application state (data) of the current node, given the key 
     * distribution and routing properties of the overlay.
     * @param splitKey the key of the new node that can be used to determine the split of application data
     * @return the portion of the application state that should be sent to the new node; if this functionality is not implemented or 
     * no state should be sent, the return should be null.
     */
    public ApplicationState split(OverlayID splitKey);
    
    /**
     * Called when a replicated message arrives at the local node.
     */
    public void update(MessageCallback callback);
    
    /**
     * Must create an application state object set to its initial (e.g. empty) state. Used by the replication layer to initialize 
     * its repository at remote nodes.
     */
    public ApplicationState createNew();
    
    /**
     * Called by a load manager for load balancing purposes
     */
    public int getLoad();
    
}
