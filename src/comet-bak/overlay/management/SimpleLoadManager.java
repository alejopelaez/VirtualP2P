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
 * SimpleLoadManager.java
 *
 * Created on October 14, 2008, 4:16 PM
 *
 */

package tassl.automate.overlay.management;

import java.util.Vector;
import tassl.automate.overlay.ApplicationState;
import tassl.automate.overlay.NodeInfo;
import tassl.automate.overlay.OverlayID;
import tassl.automate.overlay.OverlayService;

/**
 *
 * @author aquirozh
 */
public class SimpleLoadManager implements LoadManager {
    
    protected Vector<ApplicationState> applications = new Vector<ApplicationState>();
    protected OverlayService overlay;
    protected int loadThreshold = 0;
    
    private static SimpleLoadManager singletonInstance = null;
    
    private SimpleLoadManager(OverlayService overlayRef) {
        overlay = overlayRef;
    }
    
    public static SimpleLoadManager getInstance(OverlayService overlayRef) {
        if (singletonInstance == null) {
            singletonInstance = new SimpleLoadManager(overlayRef);
        }
        return singletonInstance;
    }
    
    public static SimpleLoadManager getInstance(int loadThresholdValue, OverlayService overlayRef) {
        if (singletonInstance == null) {
            singletonInstance = new SimpleLoadManager(overlayRef);
        }
        singletonInstance.setLoadThreshold(loadThresholdValue);
        return singletonInstance;
    }
    
    public void setLoadThreshold(int value) {
        loadThreshold = value;
    }

    public void manage(ApplicationState monitoredState) {
        applications.add(monitoredState);
    }

    public boolean allowNewNode(OverlayID nodeID) {
        int totalLoad = calculateTotalLoad();
        return (loadThreshold > 0 && (totalLoad + (totalLoad/applications.size()) < loadThreshold));
    }

    public NodeInfo selectHostNode(OverlayID nodeID) {
        return overlay.getNeighborhood().getSuccessor(0);
    }
    
    private int calculateTotalLoad() {
        int sum = 0;
        for (ApplicationState application : applications) {
            sum += application.getLoad();
        }
        return sum;
    }
    
}
