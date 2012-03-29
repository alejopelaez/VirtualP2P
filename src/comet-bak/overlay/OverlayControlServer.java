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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Hashtable;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import programming5.io.Debug;
import programming5.io.ArgHandler;
import programming5.io.FileHandler;
import programming5.net.MalformedMessageException;
import programming5.net.Message;
import programming5.net.MessageArrivedEvent;
import programming5.net.NetworkException;
import programming5.net.PluggableClient;
import programming5.net.MessagingClient;
import programming5.net.ServiceObject;
import programming5.net.ServiceObjectFactory;
import programming5.net.TerminationAwareSubscriber;
import programming5.net.ServerDaemon;
import programming5.net.sockets.TCPClient;
import programming5.net.sockets.TCPServerDaemon;
import tassl.automate.util.SSLClient;
import tassl.automate.util.SSLServerDaemon;

/**
 * An overlay control server is a generic server that is run at hosts where overlay objects will be
 * created. An overlay control server instance is designed to manage multiple overlay object types, as
 * long as the OverlayObject and OverlayObjectControl interfaces are correctly implemented.
 * @author Andres
 */
public class OverlayControlServer implements ServiceObjectFactory {
    Boolean ssl = false;
    ServerDaemon serverDaemon;

    public static final String regHostFileName = "/root/SVN/lib/os_reghost.txt";
    public static final String localHostFileName = "/root/SVN/hostn.txt";
    public static final int registrationPort = 5442;

    /**
     * Starts the server at the given control port, which will be used by an overlay control interface
     * object to connect to it
     */
    public OverlayControlServer(int controlPort, Boolean ssl) throws NetworkException {
        this.ssl = ssl;

        if(ssl){
            serverDaemon = new SSLServerDaemon(this, controlPort);
			System.out.println("ControlServer: Creating an SSL server daemon");
		}
        else{
            serverDaemon = new TCPServerDaemon(this, controlPort);
			System.out.println("ControlServer: Creating an TCP server daemon");
		}

        serverDaemon.start();
        String registrationURI = null;
        try {
            FileHandler regHostFile = new FileHandler(regHostFileName, FileHandler.HandleMode.READ);
            String registrationHost = regHostFile.readLine();
            if (registrationHost != null) {
                System.out.println("Registering");
                registrationURI = "//" + registrationHost + ":" + Integer.toString(controlPort+1);
            }
            regHostFile.close();
        }
        catch (IOException ioe) {
            //ioe.printStackTrace();
        }
        if (registrationURI != null) {
            try {
                String localAddress = null;
                try {
                    FileHandler localHostFile = new FileHandler(localHostFileName, FileHandler.HandleMode.READ);
                    localAddress = localHostFile.readLine();
                    localHostFile.close();
                }
                catch (IOException ioe) {
                    ioe.printStackTrace();
                }
                if (localAddress == null) {
                    localAddress = InetAddress.getLocalHost().getHostAddress();
                }
                String overlayURI = "//" + localAddress + ":" + Integer.toString(registrationPort);

                MessagingClient registrationClient;
                if(ssl)
                    registrationClient = new SSLClient();
                else
                    registrationClient = new TCPClient();

                OverlayControlMessage registrationMessage = new OverlayControlMessage(overlayURI, "Register");
                System.out.println("Sending registration message " + registrationMessage + " to " + registrationURI);
                registrationClient.send(registrationMessage.getMessageBytes(), registrationURI);
            }
            catch (UnknownHostException uhe) {
                throw new NetworkException(uhe.getMessage());
            }
        }
    }

    /**
     * Starts the server at the given control port, which will be used by an overlay control interface
     * object to connect to it
     */
    public OverlayControlServer(int controlPort) throws NetworkException {
        this(controlPort, false);
    }

    /**
     * Starts the server at the given control port, which will be used by an overlay control interface
     * object to connect to it. It first sends a registration message to the registration host given,
     * which is expected to be listening on a port that depends on the control port (currently controlPort+1)
     * @param controlPort port on which the control server listens for incoming connections
     * @param registrationHost the IP or hostname where the server must send a registration message
     * @param overlayPort the port that will be used by the overlay object, which is included in the registration message
     */
    public OverlayControlServer(int controlPort, String registrationHost, int overlayPort) throws NetworkException {
        if(ssl)
            serverDaemon = new SSLServerDaemon(this, controlPort);
        else
            serverDaemon = new TCPServerDaemon(this, controlPort);

        serverDaemon.start();
        String registrationURI = "//" + registrationHost + ":" + Integer.toString(controlPort+1);
        try {
            String localAddress = InetAddress.getLocalHost().getHostAddress();
            String overlayURI = "//" + localAddress + ":" + Integer.toString(overlayPort);

            MessagingClient registrationClient;
            if(ssl)
                registrationClient = new SSLClient();
            else
                registrationClient = new TCPClient();

            OverlayControlMessage registrationMessage = new OverlayControlMessage(overlayURI, "Register");
            registrationClient.send(registrationMessage.getMessageBytes(), registrationURI);
        }
        catch (UnknownHostException uhe) {
            throw new NetworkException(uhe.getMessage());
        }
    }

    public ServiceObject getServiceObject() {
        return new OverlayControlObject();
    }

    public static void main(String[] args) {
        try {
            System.out.println("OverlayControlServer running...");
            Debug.enable("tassl.automate.overlay.OverlayControlServer");
            ArgHandler handler = new ArgHandler(args);
            System.out.println(handler.getSwitchArg("-ssl"));

            new OverlayControlServer(Integer.parseInt(args[0]), handler.getSwitchArg("-ssl"));
        }
        catch (NetworkException ne) {
            ne.printStackTrace();
        }
    }

    /**
     * Service object implementation that realizes the functionality of the overlay control server
     * as it communicates with an overlay control interface.
     */
    protected class OverlayControlObject implements ServiceObject, TerminationAwareSubscriber<MessageArrivedEvent> {

        MessagingClient serviceConnection;
        OverlayObjectControl launcher = null;
        OverlayObject overlayObject = null;

        public OverlayControlObject() {

        }

        public synchronized void newClient(PluggableClient client) {
                serviceConnection = (MessagingClient) client;
                serviceConnection.addListener(this);
                Debug.println("New client", "tassl.automate.overlay.OverlayControlServer");
            }

        public synchronized void signalEvent(MessageArrivedEvent event) {
                try {
                    OverlayControlMessage controlMessage = new OverlayControlMessage(event.getContentBytes());
                    if (controlMessage.isInitMessage()) {
                        System.out.println("Initializing object of type " + controlMessage.getLauncherClassName());
                        Debug.println("Initializing object of type " + controlMessage.getLauncherClassName(), "tassl.automate.overlay.OverlayControlServer");
                        Hashtable<String, String> properties = controlMessage.getPropertyTable();
                        String initStatus;
                        try {
                            launcher = (OverlayObjectControl) Class.forName(controlMessage.getLauncherClassName()).newInstance();
                            Debug.println("Using property table: " + properties, "tassl.automate.overlay.OverlayControlServer");
                            overlayObject = launcher.startInstance(controlMessage.getNodeFile(), properties);
                            initStatus = "Joined";
                        }
                        catch (ClassNotFoundException cnfe) {
                            initStatus = "Failed: No launcher";
                        }
                        catch (IllegalAccessException iae) {
                            initStatus = "Failed: Bad launcher";
                        }
                        catch (InstantiationException ie) {
                            initStatus = "Failed: Instance error";
                        }
                        OverlayControlMessage reply = new OverlayControlMessage(properties.get("chord.LOCAL_URI"), initStatus);
                        serviceConnection.send(reply.getMessageBytes());
                    }
                    else if (controlMessage.isExecuteMessage()) {
                        Debug.println("Executing command " + controlMessage.getMethodName(), "tassl.automate.overlay.OverlayControlServer");
                        try {
                            Serializable result = execute(controlMessage.getMethodName(), controlMessage.getMethodParameters());
                            Debug.println("Completed command", "tassl.automate.overlay.OverlayControlServer");
                            Message reply = controlMessage.createReply(result);
                            if (reply != null) {
                                Debug.println("Sending command result", "tassl.automate.overlay.OverlayControlServer");
                                serviceConnection.send(reply.getMessageBytes());
                            }
                        }
                        catch (NetworkException ne) {
                            ne.printStackTrace();
                        }
                        catch (Exception e) {
                            if (e instanceof InvocationTargetException) {
                                e = new Exception(((InvocationTargetException) e).getTargetException());
                            }
                            Message reply = controlMessage.createReply(e);
                            serviceConnection.send(reply.getMessageBytes());
                        }
                    }
                    else if (controlMessage.isTerminateMessage()) {
                        Debug.println("Terminating object", "tassl.automate.overlay.OverlayControlServer");
                        launcher.terminateInstance(overlayObject);
                        serviceConnection.endConnection();
                    }
                }
                catch (MalformedMessageException ex) {
                    System.err.println("Bad message received: " + ex.getMessage());
                    System.out.println("OverlayControlServer running...");
                }
                catch (NetworkException ne) {
                    System.err.println("Network error: " + ne.getMessage());
                    System.out.println("OverlayControlServer running...");
                }
            }

        public Serializable execute(String methodName, Object... parameters) throws Exception {
            System.out.println("start execute")            ;
            try {
                Serializable ret = null;
                if (overlayObject != null) {
                    System.out.println("overlayObject !=null")                    ;
                    Class ooClass = overlayObject.getClass();
                    Method[] ooMethods = ooClass.getMethods();
                    Vector<Method> methodMatches = new Vector<Method>();
                    for (Method ooMethod : ooMethods) {
                        if (ooMethod.getName().equals(methodName)) {
                            methodMatches.add(ooMethod);
                        }
                    }
                    if (methodMatches.size() > 0) {
                        boolean invoked = false;
                        for (Method method : methodMatches) {
                            Class[] expectedParameters = method.getParameterTypes();
                            if (expectedParameters.length == parameters.length) {
                                boolean invokable = true;
                                for (int i = 0; i < expectedParameters.length; i++) {
                                    if (!expectedParameters[i].isAssignableFrom(parameters[i].getClass())
                                        && !matchesPrimitive(expectedParameters[i], parameters[i].getClass())) {
                                        invokable = false;
                                        break;
                                    }
                                }
                                System.out.println("before invoke")                                ;
                                if (invokable) {
                                    System.out.println("invokable")                                    ;
                                    ret = (Serializable) method.invoke(overlayObject, parameters);
                                    invoked = true;
                                    break;
                                }
                            }
                        }
                        System.out.println("almost done")                        ;
                        if (!invoked) {
                            throw new IllegalArgumentException("OverlayControlServer: Cannot execute command: Parameter types don't match");
                        }
                    }
                    else {
                        throw new NoSuchMethodError();
                    }
                }
                else {
                    System.out.println("overlayObject is null");
                    throw new IllegalStateException("OverlayControlServer: Cannot execute command: Instance not initiated");
                }
                return ret;
            }
            catch (ClassCastException cce) {
                throw new UnsupportedOperationException("OverlayControlServer: Cannot execute command: Return parameter not serializable");
            }
        }

        public void noMoreEvents() {

        }

        public void subscriptionTerminated() {
            Debug.println("Terminating object", "tassl.automate.overlay.OverlayControlServer");
            if (overlayObject != null) {
                launcher.terminateInstance(overlayObject);
            }
        }

        private boolean matchesPrimitive(Class primitive, Class derived) {
            boolean ret = false;
            if (primitive.isPrimitive()) {
                String primName = primitive.getName();
                if (primName.equals("int")) {
                    ret = (Integer.class.isAssignableFrom(derived));
                }
                else if (primName.equals("long")) {
                    ret = (Long.class.isAssignableFrom(derived));
                }
                else if (primName.equals("float")) {
                    ret = (Float.class.isAssignableFrom(derived));
                }
                else if (primName.equals("double")) {
                    ret = (Double.class.isAssignableFrom(derived));
                }
                else if (primName.equals("boolean")) {
                    ret = (Boolean.class.isAssignableFrom(derived));
                }
            }
            return ret;
        }

    }

}
