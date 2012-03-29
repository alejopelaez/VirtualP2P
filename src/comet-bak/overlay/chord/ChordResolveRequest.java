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
 * ChordResolveRequest.java
 *
 * Created on February 8, 2007, 11:09 AM
 */

package tassl.automate.overlay.chord;

import java.util.Vector;
import java.util.concurrent.TimeUnit;
import programming5.collections.MathSet;
import programming5.concurrent.ConditionVariable;
import tassl.automate.overlay.*;

/**
 * Utility class for chord that encapsulates a condition upon which objects can wait, representing the
 * resolution of a resolve request with multiple results. Although usually a chord resolve request has
 * a single solution, a solution for a chord ID cluster or in a two-level chord can have more than one
 * result.
 * @author aquirozh
 */
public class ChordResolveRequest {
    
    protected final Vector<NodeInfo> result = new Vector<NodeInfo>();
    protected ConditionVariable condition = new ConditionVariable();
    protected int targetSize;
    
    /**
     * Creates a new request that will be complete when the number of results received equals the 
     * expected size
     */
    public ChordResolveRequest(int expectedSize) {
        targetSize = expectedSize;
    }
    
    public boolean isDone() {
        return condition.isTrue();
    }
    
    /**
     * Blocks until the condition is reached (the expected number of results is received) or a timeout 
     * is reached
     * @return true if the condition was met; false otherwise (timeout)
     */
    public boolean waitOn(long timeout, TimeUnit unit) {
        boolean signaled = false;
        try {
            signaled = condition.await(timeout, unit);
        }
        catch (InterruptedException ie) {
            ie.printStackTrace();
        }
        return signaled;
    }
    
    /**
     * Called to signal a new result
     */
    public void addResult(NodeInfo nodeInfo) {
        synchronized (result) {
            result.add(nodeInfo);
            condition.evaluateCondition(result.size() >= targetSize);
        }
    }
    
    /**
     * @return the request results
     */
    public NodeInfo[] getResult() {
        MathSet<NodeInfo> resultSet;
        synchronized (result) {
            resultSet = new MathSet<NodeInfo>(result);
        }
        NodeInfo[] ret = new NodeInfo[resultSet.size()];
        int i = 0;
        for (NodeInfo element : resultSet) {
            ret[i++] = element;
        }
        return ret;
    }
    
    public long getTimeWaited() {
        return condition.getTimeWaited();
    }
    
}
