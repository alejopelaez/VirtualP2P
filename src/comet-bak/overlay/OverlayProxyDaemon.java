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
 * OverlayProxyDaemon.java
 *
 * Created on November 11, 2008, 2:36 PM
 *
 */

package tassl.automate.overlay;

import programming5.net.NetworkException;
import programming5.net.ServiceObject;
import programming5.net.ServiceObjectFactory;
import programming5.net.sockets.TCPServerDaemon;

/**
 * An overlay service implementation that accepts remote connections through an overlay proxy must create an run a thread to listen 
 * for incoming proxy object connections and to assign a proxy server to each. This class extends the TCPServerDaemon thread for this 
 * purpose, by accepting an overlay service that will be passed to the proxy server objects that will manege each connection.
 * @author aquirozh
 */
public class OverlayProxyDaemon extends TCPServerDaemon {
    
    /**
     * Creates a daemon thread listening on any available port. This port can be queried through the getLocalPort method and announced 
     * to clients.
     */
    public OverlayProxyDaemon(final OverlayService overlayRef) throws NetworkException {
        super(new ServiceObjectFactory() {
            public ServiceObject getServiceObject() {
                return new OverlayProxyServer(overlayRef);
            }
        });
    }
    
    /**
     * Creates a daemon thread listening on the given port.
     */
    public OverlayProxyDaemon(final OverlayService overlayRef, int localPort) throws NetworkException {
        super(new ServiceObjectFactory() {
            public ServiceObject getServiceObject() {
                return new OverlayProxyServer(overlayRef);
            }
        }, localPort);
    }
    
}
