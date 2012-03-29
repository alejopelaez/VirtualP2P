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
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package tassl.automate.overlay.chord;

import java.io.IOException;
import java.util.Hashtable;
import tassl.automate.overlay.OverlayObjectControl;

/**
 * Implementation of the OverlayObjectControl interface for the Chord overlay service
 * @author Andres
 */
public class ChordServiceControl implements OverlayObjectControl<ChordOverlayService> {

    /**
     * @param nodeFile the encoding of the finger table that contains the state of the chord instance
     * @param properties the property table to be used to configure the new instance
     * @return the chord service object, in a joined state
     * @throws InstantiationException if the new instance could not be created with the given parameters
     */
    public ChordOverlayService startInstance(byte[] nodeFile, Hashtable<String, String> properties) throws InstantiationException {
        try {
            ChordOverlayService ret = new ChordOverlayService(nodeFile, properties);
            return ret;
        }
        catch (IOException ioe) {
            throw new InstantiationException();
        }
    }

    /**
     * Calls the leave method on the given chord service instance
     */
    public void terminateInstance(ChordOverlayService instance) {
        instance.leave();
    }

}
