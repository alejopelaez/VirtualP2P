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

/**
 * Identifies a service interested in receiving only messages from the overlay
 * @author ahernandez
 */
public interface OverlayMessageListener {
	
	/**
	 * @param message the message received on the overlay for the subscribed tag
	 */
	public void messageArrived(MessageCallback c);
	
	/**
	 * @param where contact info of the node where the error occurred
	 * @param errorMessage the application-specific error message
	 */
	public void serviceErrorOcurred(NodeInfo where, byte[] errorMessage);
        
        /**
         * Called by the overlay service to which the implementing object has subscribed when a virtual node is being created. 
         * Implementing classes should return a new and separate instance of the same class, which will be subscribed to the new 
         * overlay service reference (implementing classes should not resubscribe the new instances created by this method to the 
         * overlay, as this will be done automatically given the current subscriptions).
         * @param serviceRef the reference of the new virtual overlay service
         */
        public OverlayMessageListener createNew(OverlayService serviceRef);
	
}
