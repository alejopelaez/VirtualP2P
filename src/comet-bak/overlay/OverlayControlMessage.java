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

package tassl.automate.overlay;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.Hashtable;
import programming5.io.Serializer;
import programming5.net.MalformedMessageException;
import programming5.net.Message;

/**
 * Message class used by the overlay control objects
 * @author Andres
 */
public class OverlayControlMessage extends Message {

    public static final String INIT_MSG = "OCM_INIT";
    public static final String URI_MSG = "OCM_URI";
    public static final String TERMINATE_MSG = "OCM_TERM";
    public static final String EXEC_MSG = "OCM_EXEC";
    public static final String RESULT_MSG = "OCM_RSLT";

    public OverlayControlMessage(byte[] messageBytes) throws MalformedMessageException {
        super(messageBytes);
    }

    public OverlayControlMessage(String launcherClassName, byte[] nodeFile, Hashtable<String, String> propertyTable) {
        super();
        this.setHeader(INIT_MSG);
        this.addMessageItem(launcherClassName);
        this.addMessageItem(nodeFile);
        try {
            this.addMessageItem(Serializer.serializeBytes(propertyTable));
        }
        catch (IOException ioe) {
            throw new RuntimeException(ioe.getMessage());
        }
    }

    public OverlayControlMessage(String joinedURI, String status) {
        super();
        this.setHeader(URI_MSG);
        this.addMessageItem(joinedURI);
        this.addMessageItem(status);
    }

    public OverlayControlMessage() {
        super();
        this.setHeader(TERMINATE_MSG);
    }

    public OverlayControlMessage(String launcherClassName) {
        super();
        this.setHeader(TERMINATE_MSG);
        this.addMessageItem(launcherClassName);
    }

    public OverlayControlMessage(BigInteger requestRef, String methodName, Serializable... parameters) {
        super();
        this.setHeader(EXEC_MSG);
        this.addMessageItem(requestRef.toString());
        this.addMessageItem(methodName);
        for (Serializable parameter : parameters) {
            try {
                this.addMessageItem(Serializer.serializeBytes(parameter));
            }
            catch (IOException ex) {
                throw new RuntimeException(ex.getMessage());
            }
        }
    }

    public boolean isInitMessage() {
        return (this.header.equals(INIT_MSG));
    }

    public String getLauncherClassName() {
        return this.getMessageItem(0);
    }

    public byte[] getNodeFile() {
        return this.getItemAsByteArray(1);
    }

    public Hashtable<String, String> getPropertyTable() throws MalformedMessageException {
        try {
            return (Hashtable<String, String>) Serializer.deserialize(this.getItemAsByteArray(2));
        }
        catch (IOException ioe) {
            throw new MalformedMessageException("OverlayControlMessage: Could not parse property table: " + ioe.getMessage());
        }
        catch (ClassNotFoundException cnfe) {
            throw new MalformedMessageException("OverlayControlMessage: Could not create property table: " + cnfe.getMessage());
        }
    }

    public boolean isURIMessage() {
        return (this.header.equals(URI_MSG));
    }

    public String getURI() {
        return this.getMessageItem(0);
    }

    public String getURIStatus() {
        return this.getMessageItem(1);
    }

    public boolean isTerminateMessage() {
        return (this.header.equals(TERMINATE_MSG));
    }

    public boolean isExecuteMessage() {
        return (this.header.equals(EXEC_MSG));
    }

    public String getMethodName() {
        return this.getMessageItem(1);
    }

    public Object[] getMethodParameters() throws MalformedMessageException {
        int howMany = this.getMessageSize() - 2;
        Object[] ret = new Object[howMany];
        for (int i = 0; i < howMany; i++) {
            try {
                ret[i] = Serializer.deserialize(this.getItemAsByteArray(i+2));
            }
            catch (IOException ioe) {
                throw new MalformedMessageException("OverlayControlMessage: Could not parse parameter: " + ioe.getMessage());
            }
            catch (ClassNotFoundException cnfe) {
                throw new MalformedMessageException("OverlayControlMessage: Could not create parameter object: " + cnfe.getMessage());
            }
        }
        return ret;
    }

    public Message createReply(Serializable result) {
        try {
            Message ret = null;
            String replyRef = this.getMessageItem(0);
            if (!BigInteger.ZERO.equals(new BigInteger(replyRef))) {
                if (result != null) {
                    ret = Message.constructMessage(RESULT_MSG, replyRef, Serializer.serializeBytes(result));
                }
                else {
                    ret = Message.constructMessage(RESULT_MSG, replyRef);
                }
            }
            return ret;
        }
        catch (IOException ex) {
            throw new RuntimeException(ex.getMessage());
        }
    }

    public boolean isResultMessage() {
        return (this.header.equals(RESULT_MSG));
    }

    public BigInteger getRequestReference() {
        return new BigInteger(this.getMessageItem(0));
    }

    public boolean hasResult() {
        return (this.getMessageSize() > 1);
    }

    public Object getResult() throws MalformedMessageException {
        try {
            return Serializer.deserialize(this.getItemAsByteArray(1));
        }
        catch (IOException ex) {
                throw new MalformedMessageException("OverlayControlMessage: Could not parse result: " + ex.getMessage());
        }
        catch (ClassNotFoundException ex) {
                throw new MalformedMessageException("OverlayControlMessage: Could not create result object: " + ex.getMessage());
        }
    }

}
