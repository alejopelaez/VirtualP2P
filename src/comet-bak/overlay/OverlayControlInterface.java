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
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import programming5.collections.HashTable;
import programming5.collections.MultiVector;
import programming5.collections.keyGenerators.BigIntegerGenerator;
import programming5.concurrent.MultiRequestVariable;
import programming5.concurrent.RequestVariable;
import programming5.io.Debug;
import programming5.io.FileHandler;
import programming5.net.IncompleteResultException;
import programming5.net.MalformedMessageException;
import programming5.net.MessageArrivedEvent;
import programming5.net.MessageArrivedListener;
import programming5.net.NetworkException;
import programming5.net.PluggableClient;
import programming5.net.MessagingClient;
import programming5.net.ServiceObject;
import programming5.net.ServerAcceptThread;
import programming5.net.sockets.TCPClient;
import programming5.net.sockets.TCPServerAcceptThread;
import tassl.automate.overlay.chord.ChordID;
import tassl.automate.util.SSLClient;
import tassl.automate.util.SSLServerAcceptThread;
import tassl.automate.util.ReturnObjectHandler;
import tassl.automate.util.SimulationHelper;

/**
 * The overlay control interface for a chord-based overlay object is a class responsible for starting and
 * controlling all of the distributed instances of that overlay object from a single node. It requires
 * the overlay object class and overlay object control, which provide the logic to create and initialize
 * the overlay objects and the network stacks that they need to communicate. This class works in concert
 * with an overlay control server, which must run on the remote nodes where the remote object instances
 * will be created.
 * @author Andres
 */
public class OverlayControlInterface<E extends OverlayObject> implements MessageArrivedListener, ServiceObject {

    protected MultiRequestVariable<String> nodesJoined = null;
    protected Hashtable<String, MessagingClient> remoteClients = new Hashtable<String, MessagingClient>();
    protected final HashTable<BigInteger, RequestVariable<Object>> executeRequests = new HashTable<BigInteger, RequestVariable<Object>>();
    protected final HashTable<BigInteger, ReturnObjectHandler> asyncExecuteRequests = new HashTable<BigInteger, ReturnObjectHandler>();

    protected String localNodeURI = null;

    private int controlPort;
    private boolean reuseFiles = false;

    private BigIntegerGenerator requestKeyGenerator = new BigIntegerGenerator(64);

    /**
     * @param myControlPort the port that will be used to communicate with the overlay control servers
     */
    public OverlayControlInterface(int myControlPort) {
        controlPort = myControlPort;
    }

    /**
     * @param myControlPort the port that will be used to communicate with the overlay control servers
     * @param reuseNodeFiles if true, will look for existing node files to use as parameters for object
     * creation; otherwise, will generate new node files for the overlay objects according to the
     * start method parameters.
     */
    public OverlayControlInterface(int myControlPort, boolean reuseNodeFiles) {
        controlPort = myControlPort;
        reuseFiles = reuseNodeFiles;
    }

    /**
     * Meant for use in a P2P network or public cloud, where the identity and/or location of nodes is
     * not known beforehand and therefore must be discovered as the nodes join.
     * @param n the number of nodes to wait for
     * @param timeout
     * @return the URI's of the nodes that have joined by the timeout; will block for the minimum of the
     * timeout value or the time it takes for n nodes to join.
     */
    public Vector<String> waitForNodes(int n, long timeout) {
        nodesJoined = new MultiRequestVariable<String>(n);
        try {
            ServerAcceptThread registrationAccepter;
			String networkClass = System.getProperty("chord.NETWORK_CLASS", "TCP");

			if(networkClass.equals("SSL"))
				registrationAccepter = new SSLServerAcceptThread(this, controlPort+1);
			else
				registrationAccepter = new TCPServerAcceptThread(this, controlPort+1);

            registrationAccepter.start();
        }
        catch (NetworkException ex) {
            ex.printStackTrace();
        }
        System.out.println("Waiting for nodes to register");
        nodesJoined.awaitUninterruptibly(timeout, TimeUnit.MILLISECONDS);
        System.out.println("All nodes registered");
        return new Vector<String>(nodesJoined.getResult());
    }

    /**
     * The result of the invocation of this method is the creation and initialization of all instances
     * of the overlay object, local and remote, so that the overlay is ready to operate and respond to
     * (route) requests. This method always creates a local instance and returns a handle to it as a
     * local interface to the overlay.
     * @param controlClass the overlay object control class type, which will be used to launch a control object to create overlay object instances
     * @param nodeDescriptors node descriptors are property tables that are used as parameters for overlay object creation. The map given must contain a node descriptor for each node where overlay objects will be created
     * @param localNodeURI the URI of the local node, where the local instance will be created; must correspond to one of the URIs that index the node descriptor table
     * @param timeout this method returns when the entire overlay is ready; since this operation may fail, the user can specify a timeout value, after which the method will throw an IncompleteResultException
     * @return the reference to the local overlay object instance, that can immediately be used as an interface to the overlay
     * @throws URISyntaxException
     * @throws InstantiationException if the overlay objects could not be created with the object control class or parameters given
     * @throws IncompleteResultException if the method times out before the creation and initialization of the overlay is complete
     */
    public E startAsBootstrap(Class<? extends OverlayObjectControl> controlClass, Map<String, Hashtable<String, String>> nodeDescriptors, String localNodeURI, long timeout) throws URISyntaxException, InstantiationException, IncompleteResultException {
        try {
            E bootstrapObject = null;
            // Initialize local variables
            Vector<String> nodeURIList = new Vector<String>(nodeDescriptors.keySet());
            System.out.println("==> nodeURIList: "+nodeURIList);
            this.localNodeURI = localNodeURI;
            nodesJoined = new MultiRequestVariable<String>(nodeURIList.size()-1);
            int idBits = Integer.parseInt(nodeDescriptors.get(localNodeURI).get("chord.ID_BITS"));
            // Create chord file names
            MultiVector<String, String> nodeControlList = new MultiVector<String, String>();
            MultiVector<String, String> nodeOverlayList = new MultiVector<String, String>();
            boolean filesExist = true;
            for (String uriString : nodeURIList) {
                URI uri = new URI(uriString);
                String nodeFileName = uri.getHost() + "_" + Integer.toString(uri.getPort()) + ".chn";
                nodeControlList.add(nodeFileName, "//" + uri.getHost() + ":" + Integer.toString(controlPort));
                nodeOverlayList.add(nodeFileName, uriString);
                if (!FileHandler.fileExists(nodeFileName)) {
                    filesExist = false;
                }
            }
            // Create chord files if not found
            if (!filesExist || !reuseFiles) {
                SimulationHelper helper = new SimulationHelper();
                helper.createNodeFiles(nodeURIList, idBits);
            }
            // Create ring
            try {
                for (String nodeFileName : nodeControlList.first()) {
                    FileHandler nodeFile = new FileHandler(nodeFileName, FileHandler.HandleMode.READ, FileHandler.FileType.BINARY);
                    String nodeURI = nodeOverlayList.getInSecond(nodeFileName);
                    Hashtable<String, String> nodeProperties = nodeDescriptors.get(nodeURI);
                    nodeProperties.put("chord.LOCAL_URI", nodeURI);
                    nodeProperties.put("chord.LOCAL_BOOTSTRAP", localNodeURI);
                    if (nodeURI.equals(localNodeURI)) {
                        // Launch bootstrap node
                        OverlayObjectControl<E> bootstrapLauncher = (OverlayObjectControl<E>) controlClass.newInstance();
                        bootstrapObject = bootstrapLauncher.startInstance(nodeFile.readFully(), nodeProperties);
                    }
                    else {
						MessagingClient initClient = getClient();
                        initClient.addListener(this);
                        remoteClients.put(nodeOverlayList.getInSecond(nodeFileName), initClient);
                        OverlayControlMessage initMessage = new OverlayControlMessage(controlClass.getName(), nodeFile.readFully(), nodeProperties);
                        String destURI = nodeControlList.getInSecond(nodeFileName);
                        //System.out.println("==>destURI="+destURI+" nodeFileName="+nodeFileName);

                        try {
                            Debug.println("Sending init message", "overlayControl");
                            Debug.println(nodeProperties, "overlayControl");
                            initClient.send(initMessage.getMessageBytes(), destURI);
                        }
                        catch (NetworkException ne) {
                            System.err.println("Error sending to " + destURI + ": " + ne.getMessage());
                        }
                    }
                }
                // Wait for join replies
                Debug.println("To wait");
                if (!nodesJoined.awaitUninterruptibly(timeout, TimeUnit.MILLISECONDS)) {
                    Collection<String> joinedList = nodesJoined.getResult();
                    Hashtable<String, MessagingClient> auxClientTable = new Hashtable<String, MessagingClient>();
                    for (String joinedNode : joinedList) {
                        auxClientTable.put(joinedNode, remoteClients.get(joinedNode));
                    }
                    remoteClients = auxClientTable;
                    throw new IncompleteResultException(bootstrapObject);
                }
                Debug.println("Done waiting");
            }
            catch (IllegalAccessException iae) {
                throw new InstantiationException(iae.getMessage());
            }
            return bootstrapObject;
        }
        catch (UnknownHostException uhe) {
            uhe.printStackTrace();
            throw new RuntimeException("Unexpected error: Unknown host");
        }
        catch (IOException ioe) {
            ioe.printStackTrace();
            throw new RuntimeException("Unexpected error: Node file exception");
        }
        catch (NumberFormatException nfe) {
            throw new RuntimeException("chord.ID_BITS property must be set");
        }
    }

    /**
     * The result of the invocation of this method is the creation and initialization of all instances
     * of the overlay object, local and remote, so that the overlay is ready to operate and respond to
     * (route) requests. This method always creates a local instance and returns a handle to it as a
     * local interface to the overlay.
     * @param controlClass the overlay object control class type, which will be used to launch a control object to create overlay object instances
     * @param hostList IPs or host names of all of the physical nodes that will host overlay objects
     * @param portList the size of the port list indicates the number of objects that will be hosted per physical node, so that each one will be assigned a port number from the port list
     * @param localNodeURI a URI that corresponds to the object instance on the local node; it must correspond to a hostname:port combination obtainable the host and port lists
     * @param localProperties property table that will be used to parameterize the creation of the local overlay object
     * @param remoteProperties property table that will be used to parameterize the creation of all remote overlay objects
     * @param timeout this method returns when the entire overlay is ready; since this operation may fail, the user can specify a timeout value, after which the method will throw an IncompleteResultException
     * @return the reference to the local overlay object instance, that can immediately be used as an interface to the overlay
     * @throws URISyntaxException
     * @throws InstantiationException if the overlay objects could not be created with the object control class or parameters given
     * @throws IncompleteResultException if the method times out before the creation and initialization of the overlay is complete
     */
    public E startAsBootstrap(Class<? extends OverlayObjectControl> controlClass, Vector<String> hostList, Vector<Integer> portList, String localNodeURI, Hashtable<String, String> localProperties, Hashtable<String, String> remoteProperties, long timeout) throws URISyntaxException, InstantiationException, IncompleteResultException {
        Vector<String> nodeURIList = new Vector<String>();
        this.localNodeURI = localNodeURI;
        for (String address : hostList) {
            for (Integer port : portList) {
                nodeURIList.add("//" + address + ":" + Integer.toString(port));
            }
        }
        MultiVector<String, Hashtable<String, String>> nodeDescriptors = new MultiVector<String, Hashtable<String, String>>();
        for (String node : nodeURIList) {
            if (!node.equals(localNodeURI)) {
                nodeDescriptors.add(node, remoteProperties);
            }
            else {
                nodeDescriptors.add(node, localProperties);
            }
        }
        return startAsBootstrap(controlClass, nodeDescriptors, localNodeURI, timeout);
    }

    /**
     * The result of the invocation of this method is the creation and initialization of all instances
     * of the overlay object, local and remote, so that the overlay is ready to operate and respond to
     * (route) requests. This method always creates a local instance and returns a handle to it as a
     * local interface to the overlay.
     * @param controlClass the overlay object control class type, which will be used to launch a control object to create overlay object instances
     * @param nodeDescriptors node descriptors are property tables that are used as parameters for overlay object creation. The map given must contain a node descriptor for each node where overlay objects will be created
     * @param localNodeURI the URI of the local node, where the local instance will be created; must correspond to one of the URIs that index the node descriptor table
     * @param idList a list of ChordIDs that will be used to create the finger tables of the underlying chord objects of each overlay object; used to avoid having chord assign a random ID to each object
     * @param timeout this method returns when the entire overlay is ready; since this operation may fail, the user can specify a timeout value, after which the method will throw an IncompleteResultException
     * @return the reference to the local overlay object instance, that can immediately be used as an interface to the overlay
     * @throws URISyntaxException
     * @throws InstantiationException if the overlay objects could not be created with the object control class or parameters given
     * @throws IncompleteResultException if the method times out before the creation and initialization of the overlay is complete
     */
    public E startAsBootstrap(Class<? extends OverlayObjectControl> controlClass, Map<String, Hashtable<String, String>> nodeDescriptors, String localNodeURI, Vector<ChordID> idList, long timeout) throws URISyntaxException, InstantiationException, IncompleteResultException {
        try {
            E bootstrapObject = null;
            // Initialize local variables
            Vector<String> nodeURIList = new Vector<String>(nodeDescriptors.keySet());
            this.localNodeURI = localNodeURI;
            nodesJoined = new MultiRequestVariable<String>(nodeURIList.size()-1);
            // Create chord file names
            MultiVector<String, String> nodeControlList = new MultiVector<String, String>();
            MultiVector<String, String> nodeOverlayList = new MultiVector<String, String>();
            for (String uriString : nodeURIList) {
                URI uri = new URI(uriString);
                String nodeFileName = uri.getHost() + "_" + Integer.toString(uri.getPort()) + ".chn";
                nodeControlList.add(nodeFileName, "//" + uri.getHost() + ":" + Integer.toString(controlPort));
                nodeOverlayList.add(nodeFileName, uriString);
            }
            // Create chord files
            SimulationHelper helper = new SimulationHelper();
            helper.createNodeFiles(idList, nodeURIList);
            // Create ring
            try {
                for (String nodeFileName : nodeControlList.first()) {
                    FileHandler nodeFile = new FileHandler(nodeFileName, FileHandler.HandleMode.READ, FileHandler.FileType.BINARY);
                    String nodeURI = nodeOverlayList.getInSecond(nodeFileName);
                    Hashtable<String, String> nodeProperties = nodeDescriptors.get(nodeURI);
                    nodeProperties.put("chord.LOCAL_URI", nodeURI);
                    nodeProperties.put("chord.LOCAL_BOOTSTRAP", localNodeURI);
                    if (nodeURI.equals(localNodeURI)) {
                        // Launch bootstrap node
                        OverlayObjectControl<E> bootstrapLauncher = (OverlayObjectControl<E>) controlClass.newInstance();
                        bootstrapObject = bootstrapLauncher.startInstance(nodeFile.readFully(), nodeProperties);
                    }
                    else {
						MessagingClient initClient = getClient();
                        initClient.addListener(this);
                        remoteClients.put(nodeOverlayList.getInSecond(nodeFileName), initClient);
                        OverlayControlMessage initMessage = new OverlayControlMessage(controlClass.getName(), nodeFile.readFully(), nodeProperties);
                        String destURI = nodeControlList.getInSecond(nodeFileName);
                        try {
                            Debug.println("Sending init message", "overlayControl");
                            initClient.send(initMessage.getMessageBytes(), destURI);
                        }
                        catch (NetworkException ne) {
                            System.err.println("Error sending to " + destURI + ": " + ne.getMessage());
                        }
                    }
                }
                // Wait for join replies
                Debug.println("To wait");
                if (!nodesJoined.awaitUninterruptibly(timeout, TimeUnit.MILLISECONDS)) {
                    Collection<String> joinedList = nodesJoined.getResult();
                    Hashtable<String, MessagingClient> auxClientTable = new Hashtable<String, MessagingClient>();
                    for (String joinedNode : joinedList) {
                        auxClientTable.put(joinedNode, remoteClients.get(joinedNode));
                    }
                    remoteClients = auxClientTable;
                    throw new IncompleteResultException(bootstrapObject);
                }
                Debug.println("Done waiting");
            }
            catch (IllegalAccessException iae) {
                throw new InstantiationException(iae.getMessage());
            }
            return bootstrapObject;
        }
        catch (UnknownHostException uhe) {
            uhe.printStackTrace();
            throw new RuntimeException("Unexpected error: Unknown host");
        }
        catch (IOException ioe) {
            ioe.printStackTrace();
            throw new RuntimeException("Unexpected error: Node file exception");
        }
        catch (NumberFormatException nfe) {
            throw new RuntimeException("chord.ID_BITS property must be set");
        }
    }

    /**
     * The result of the invocation of this method is the creation and initialization of all instances
     * of the overlay object, local and remote, so that the overlay is ready to operate and respond to
     * (route) requests. This method always creates a local instance and returns a handle to it as a
     * local interface to the overlay.
     * @param controlClass the overlay object control class type, which will be used to launch a control object to create overlay object instances
     * @param hostList IPs or host names of all of the physical nodes that will host overlay objects
     * @param portList the size of the port list indicates the number of objects that will be hosted per physical node, so that each one will be assigned a port number from the port list
     * @param localNodeURI a URI that corresponds to the object instance on the local node; it must correspond to a hostname:port combination obtainable the host and port lists
     * @param idList a list of ChordIDs that will be used to create the finger tables of the underlying chord objects of each overlay object; used to avoid having chord assign a random ID to each object
     * @param localProperties property table that will be used to parameterize the creation of the local overlay object
     * @param remoteProperties property table that will be used to parameterize the creation of all remote overlay objects
     * @param timeout this method returns when the entire overlay is ready; since this operation may fail, the user can specify a timeout value, after which the method will throw an IncompleteResultException
     * @return the reference to the local overlay object instance, that can immediately be used as an interface to the overlay
     * @throws URISyntaxException
     * @throws InstantiationException if the overlay objects could not be created with the object control class or parameters given
     * @throws IncompleteResultException if the method times out before the creation and initialization of the overlay is complete
     */
    public E startAsBootstrap(Class<? extends OverlayObjectControl> controlClass, Vector<String> hostList, Vector<Integer> portList, String localNodeURI, Vector<ChordID> idList, Hashtable<String, String> localProperties, Hashtable<String, String> remoteProperties, long timeout) throws URISyntaxException, InstantiationException, IncompleteResultException {
        Vector<String> nodeURIList = new Vector<String>();
        this.localNodeURI = localNodeURI;
        for (String address : hostList) {
            for (Integer port : portList) {
                nodeURIList.add("//" + address + ":" + Integer.toString(port));
            }
        }
        MultiVector<String, Hashtable<String, String>> nodeDescriptors = new MultiVector<String, Hashtable<String, String>>();
        for (String node : nodeURIList) {
            if (!node.equals(localNodeURI)) {
                nodeDescriptors.add(node, remoteProperties);
            }
            else {
                nodeDescriptors.add(node, localProperties);
            }
        }
        return startAsBootstrap(controlClass, nodeDescriptors, localNodeURI, idList, timeout);
    }

    /**
     * The result of the invocation of this method is the creation and initialization of all instances
     * of the overlay object on all host machines, so that the overlay is ready to operate and respond to
     * (route) requests. The node where this method does not necessarily have to host an overlay object
     * (i.e. all overlay objects may be hosted on remote nodes)
     * @param controlClass the overlay object control class type, which will be used to launch a control object to create overlay object instances
     * @param nodeDescriptors node descriptors are property tables that are used as parameters for overlay object creation. The map given must contain a node descriptor for each node where overlay objects will be created
     * @param timeout this method returns when the entire overlay is ready; since this operation may fail, the user can specify a timeout value, after which the method will throw an IncompleteResultException
     * @throws URISyntaxException
     * @throws InstantiationException if the overlay objects could not be created with the object control class or parameters given
     * @throws IncompleteResultException if the method times out before the creation and initialization of the overlay is complete
     */
    public void startRemoteRing(Class<? extends OverlayObjectControl> controlClass, Map<String, Hashtable<String, String>> nodeDescriptors, long timeout) throws URISyntaxException, InstantiationException, IncompleteResultException {
        try {
            // Initialize local variables
            Vector<String> nodeURIList = new Vector<String>(nodeDescriptors.keySet());
            nodesJoined = new MultiRequestVariable<String>(nodeURIList.size());
            int idBits = Integer.parseInt(nodeDescriptors.values().iterator().next().get("chord.ID_BITS"));
            // Create chord file names
            MultiVector<String, String> nodeControlList = new MultiVector<String, String>();
            MultiVector<String, String> nodeOverlayList = new MultiVector<String, String>();
            boolean filesExist = true;
            for (String uriString : nodeURIList) {
                URI uri = new URI(uriString);
                String nodeFileName = uri.getHost() + "_" + Integer.toString(uri.getPort()) + ".chn";
                nodeControlList.add(nodeFileName, "//" + uri.getHost() + ":" + Integer.toString(controlPort));
                nodeOverlayList.add(nodeFileName, uriString);
                if (!FileHandler.fileExists(nodeFileName)) {
                    filesExist = false;
                }
            }
            // Create chord files if not found
            if (!filesExist || !reuseFiles) {
                SimulationHelper helper = new SimulationHelper();
                helper.createNodeFiles(nodeURIList, idBits);
            }
            // Create ring
            for (String nodeFileName : nodeControlList.first()) {
                FileHandler nodeFile = new FileHandler(nodeFileName, FileHandler.HandleMode.READ, FileHandler.FileType.BINARY);
                String nodeURI = nodeOverlayList.getInSecond(nodeFileName);
                Hashtable<String, String> nodeProperties = nodeDescriptors.get(nodeURI);
                nodeProperties.put("chord.LOCAL_URI", nodeURI);
				MessagingClient initClient = getClient();
                initClient.addListener(this);
                remoteClients.put(nodeOverlayList.getInSecond(nodeFileName), initClient);
                OverlayControlMessage initMessage = new OverlayControlMessage(controlClass.getName(), nodeFile.readFully(), nodeProperties);
                String destURI = nodeControlList.getInSecond(nodeFileName);
                try {
                    Debug.println("Sending init message to "+destURI, "overlayControl");
                    initClient.send(initMessage.getMessageBytes(), destURI);
                }
                catch (NetworkException ne) {
                    System.err.println("Error sending to " + destURI + ": " + ne.getMessage());
                }
            }
            // Wait for join replies
            Debug.println("To wait");
            if (!nodesJoined.awaitUninterruptibly(timeout, TimeUnit.MILLISECONDS)) {
                    Collection<String> joinedList = nodesJoined.getResult();
                    Hashtable<String, MessagingClient> auxClientTable = new Hashtable<String, MessagingClient>();
                    for (String joinedNode : joinedList) {
                        auxClientTable.put(joinedNode, remoteClients.get(joinedNode));
                    }
                    remoteClients = auxClientTable;
                    throw new IncompleteResultException(null);
            }
            Debug.println("Done waiting");
        }
        catch (UnknownHostException uhe) {
            uhe.printStackTrace();
            throw new RuntimeException("Unexpected error: Unknown host");
        }
        catch (IOException ioe) {
            ioe.printStackTrace();
            throw new RuntimeException("Unexpected error: Node file exception");
        }
        catch (NumberFormatException nfe) {
            throw new RuntimeException("chord.ID_BITS property must be set");
        }
    }

    public void terminateRemoteNodes(Class<? extends OverlayObjectControl> controlClass, Map<String, Hashtable<String, String>> nodeDescriptors, long timeout) throws URISyntaxException, InstantiationException, IncompleteResultException {
        Vector<String> nodeURIList = new Vector<String>(nodeDescriptors.keySet());
        OverlayControlMessage terminateMessage = new OverlayControlMessage();
		MessagingClient initClient = getClient();
        initClient.addListener(this);
        for (String uriString: nodeURIList) {
            URI uri = new URI(uriString);
            String destURI = "//" + uri.getHost() + ":" + Integer.toString(controlPort);
            try {
System.out.println("Sending termination message to "+destURI);
                Debug.println("Sending termination message to "+destURI, "overlayControl");
                initClient.send(terminateMessage.getMessageBytes(), destURI);
            }
            catch (NetworkException ne) {
                System.err.println("Error sending to " + destURI + ": " + ne.getMessage());
            }
        }
    }

    public void test (Class<? extends OverlayObjectControl> controlClass, Map<String, Hashtable<String, String>> nodeDescriptors, long timeout) throws URISyntaxException, InstantiationException, IncompleteResultException {
        try {
            // Initialize local variables
            Vector<String> nodeURIList = new Vector<String>(nodeDescriptors.keySet());
            nodesJoined = new MultiRequestVariable<String>(nodeURIList.size());
            int idBits = Integer.parseInt(nodeDescriptors.values().iterator().next().get("chord.ID_BITS"));
            // Create chord file names
            MultiVector<String, String> nodeControlList = new MultiVector<String, String>();
            MultiVector<String, String> nodeOverlayList = new MultiVector<String, String>();
            boolean filesExist = true;
            for (String uriString : nodeURIList) {
                URI uri = new URI(uriString);
                String nodeFileName = uri.getHost() + "_" + Integer.toString(uri.getPort()) + ".chn";
                nodeControlList.add(nodeFileName, "//" + uri.getHost() + ":" + Integer.toString(controlPort));
                nodeOverlayList.add(nodeFileName, uriString);
                if (!FileHandler.fileExists(nodeFileName)) {
                    filesExist = false;
                }
            }
            // Create chord files if not found
            if (!filesExist || !reuseFiles) {
                SimulationHelper helper = new SimulationHelper();
                helper.createNodeFiles(nodeURIList, idBits);
            }
            // Create ring
            for (String nodeFileName : nodeControlList.first()) {
//                FileHandler nodeFile = new FileHandler(nodeFileName, FileHandler.HandleMode.READ, FileHandler.FileType.BINARY);
                String nodeURI = nodeOverlayList.getInSecond(nodeFileName);
                Hashtable<String, String> nodeProperties = nodeDescriptors.get(nodeURI);
                nodeProperties.put("chord.LOCAL_URI", nodeURI);
                MessagingClient initClient = getClient();
                initClient.addListener(this);
                remoteClients.put(nodeOverlayList.getInSecond(nodeFileName), initClient);
                OverlayControlMessage terminateMessage = new OverlayControlMessage(controlClass.getName());
//RequestVariable<Object> request = new RequestVariable<Object>();
//BigInteger requestRef;
//synchronized (executeRequests) {
//    requestRef = executeRequests.randomPut(request, requestKeyGenerator);
//}
//OverlayControlMessage terminateMessage = new OverlayControlMessage(requestRef, "terminateInstance");
                String destURI = nodeControlList.getInSecond(nodeFileName);
                try {
System.out.println("Sending termination message to "+destURI);
                    Debug.println("Sending termination message to "+destURI, "overlayControl");
                    initClient.send(terminateMessage.getMessageBytes(), destURI);
                }
                catch (NetworkException ne) {
                    System.err.println("Error sending to " + destURI + ": " + ne.getMessage());
                }
            }
            // Wait for join replies
//            Debug.println("To wait");
//            if (!nodesJoined.awaitUninterruptibly(timeout, TimeUnit.MILLISECONDS)) {
//                    Collection<String> joinedList = nodesJoined.getResult();
//                    Hashtable<String, TCPClient> auxClientTable = new Hashtable<String, TCPClient>();
//                    for (String joinedNode : joinedList) {
//                        auxClientTable.put(joinedNode, remoteClients.get(joinedNode));
//                    }
//                    remoteClients = auxClientTable;
//                    throw new IncompleteResultException(null);
//            }
//            Debug.println("Done waiting");
        }
//        catch (UnknownHostException uhe) {
//            uhe.printStackTrace();
//            throw new RuntimeException("Unexpected error: Unknown host");
//        }
//        catch (IOException ioe) {
//            ioe.printStackTrace();
//            throw new RuntimeException("Unexpected error: Node file exception");
//        }
        catch (NumberFormatException nfe) {
            throw new RuntimeException("chord.ID_BITS property must be set");
        }
    }

    /**
     * The result of the invocation of this method is the creation and initialization of all instances
     * of the overlay object on all host machines, so that the overlay is ready to operate and respond to
     * (route) requests. The node where this method does not necessarily have to host an overlay object
     * (i.e. all overlay objects may be hosted on remote nodes)
     * @param controlClass the overlay object control class type, which will be used to launch a control object to create overlay object instances
     * @param hostList IPs or host names of all of the physical nodes that will host overlay objects
     * @param portList the size of the port list indicates the number of objects that will be hosted per physical node, so that each one will be assigned a port number from the port list
     * @param properties property table that will parameterize the creation of all objects in the overlay
     * @param timeout this method returns when the entire overlay is ready; since this operation may fail, the user can specify a timeout value, after which the method will throw an IncompleteResultException
     * @throws URISyntaxException
     * @throws InstantiationException if the overlay objects could not be created with the object control class or parameters given
     * @throws IncompleteResultException if the method times out before the creation and initialization of the overlay is complete
     */
    public void startRemoteRing(Class<? extends OverlayObjectControl> controlClass, Vector<String> hostList, Vector<Integer> portList, Hashtable<String, String> properties, long timeout) throws URISyntaxException, InstantiationException, IncompleteResultException {
        Vector<String> nodeURIList = new Vector<String>();
        for (String address : hostList) {
            for (Integer port : portList) {
                nodeURIList.add("//" + address + ":" + Integer.toString(port));
            }
        }
        MultiVector<String, Hashtable<String, String>> nodeDescriptors = new MultiVector<String, Hashtable<String, String>>();
        for (String node : nodeURIList) {
            nodeDescriptors.add(node, properties);
        }
        startRemoteRing(controlClass, nodeDescriptors, timeout);
    }

    /**
     * The result of the invocation of this method is the creation and initialization of all instances
     * of the overlay object on all host machines, so that the overlay is ready to operate and respond to
     * (route) requests. The node where this method does not necessarily have to host an overlay object
     * (i.e. all overlay objects may be hosted on remote nodes)
     * @param controlClass the overlay object control class type, which will be used to launch a control object to create overlay object instances
     * @param nodeDescriptors node descriptors are property tables that are used as parameters for overlay object creation. The map given must contain a node descriptor for each node where overlay objects will be created
     * @param idList a list of ChordIDs that will be used to create the finger tables of the underlying chord objects of each overlay object; used to avoid having chord assign a random ID to each object
     * @param timeout this method returns when the entire overlay is ready; since this operation may fail, the user can specify a timeout value, after which the method will throw an IncompleteResultException
     * @throws URISyntaxException
     * @throws InstantiationException if the overlay objects could not be created with the object control class or parameters given
     * @throws IncompleteResultException if the method times out before the creation and initialization of the overlay is complete
     */
    public void startRemoteRing(Class<? extends OverlayObjectControl> controlClass, Map<String, Hashtable<String, String>> nodeDescriptors, Vector<ChordID> idList, long timeout) throws URISyntaxException, InstantiationException, IncompleteResultException {
        try {
            // Initialize local variables
            Vector<String> nodeURIList = new Vector<String>(nodeDescriptors.keySet());
            nodesJoined = new MultiRequestVariable<String>(nodeURIList.size());
            int idBits = Integer.parseInt(nodeDescriptors.values().iterator().next().get("chord.ID_BITS"));
            // Create chord file names
            MultiVector<String, String> nodeControlList = new MultiVector<String, String>();
            MultiVector<String, String> nodeOverlayList = new MultiVector<String, String>();
            for (String uriString : nodeURIList) {
                URI uri = new URI(uriString);
                String nodeFileName = uri.getHost() + "_" + Integer.toString(uri.getPort()) + ".chn";
                nodeControlList.add(nodeFileName, "//" + uri.getHost() + ":" + Integer.toString(controlPort));
                nodeOverlayList.add(nodeFileName, uriString);
            }
            // Create chord files
            SimulationHelper helper = new SimulationHelper();
            helper.createNodeFiles(idList, nodeURIList);
            // Create ring
            for (String nodeFileName : nodeControlList.first()) {
                FileHandler nodeFile = new FileHandler(nodeFileName, FileHandler.HandleMode.READ, FileHandler.FileType.BINARY);
                String nodeURI = nodeOverlayList.getInSecond(nodeFileName);
                Hashtable<String, String> nodeProperties = nodeDescriptors.get(nodeURI);
                nodeProperties.put("chord.LOCAL_URI", nodeURI);
                MessagingClient initClient = getClient();
                initClient.addListener(this);
                remoteClients.put(nodeOverlayList.getInSecond(nodeFileName), initClient);
                OverlayControlMessage initMessage = new OverlayControlMessage(controlClass.getName(), nodeFile.readFully(), nodeProperties);
                String destURI = nodeControlList.getInSecond(nodeFileName);
                try {
                    Debug.println("Sending init message", "overlayControl");
                    initClient.send(initMessage.getMessageBytes(), destURI);
                }
                catch (NetworkException ne) {
                    System.err.println("Error sending to " + destURI + ": " + ne.getMessage());
                }
            }
            // Wait for join replies
            Debug.println("To wait");
            if (!nodesJoined.awaitUninterruptibly(timeout, TimeUnit.MILLISECONDS)) {
                    Collection<String> joinedList = nodesJoined.getResult();
                    Hashtable<String, MessagingClient> auxClientTable = new Hashtable<String, MessagingClient>();
                    for (String joinedNode : joinedList) {
                        auxClientTable.put(joinedNode, remoteClients.get(joinedNode));
                    }
                    remoteClients = auxClientTable;
                    throw new IncompleteResultException(null);
            }
            Debug.println("Done waiting");
        }
        catch (UnknownHostException uhe) {
            uhe.printStackTrace();
            throw new RuntimeException("Unexpected error: Unknown host");
        }
        catch (IOException ioe) {
            ioe.printStackTrace();
            throw new RuntimeException("Unexpected error: Node file exception");
        }
        catch (NumberFormatException nfe) {
            throw new RuntimeException("chord.ID_BITS property must be set");
        }
    }

    /**
     * The result of the invocation of this method is the creation and initialization of all instances
     * of the overlay object on all host machines, so that the overlay is ready to operate and respond to
     * (route) requests. The node where this method does not necessarily have to host an overlay object
     * (i.e. all overlay objects may be hosted on remote nodes)
     * @param controlClass the overlay object control class type, which will be used to launch a control object to create overlay object instances
     * @param hostList IPs or host names of all of the physical nodes that will host overlay objects
     * @param portList the size of the port list indicates the number of objects that will be hosted per physical node, so that each one will be assigned a port number from the port list
     * @param idList a list of ChordIDs that will be used to create the finger tables of the underlying chord objects of each overlay object; used to avoid having chord assign a random ID to each object
     * @param properties property table that will parameterize the creation of all objects in the overlay
     * @param timeout this method returns when the entire overlay is ready; since this operation may fail, the user can specify a timeout value, after which the method will throw an IncompleteResultException
     * @throws URISyntaxException
     * @throws InstantiationException if the overlay objects could not be created with the object control class or parameters given
     * @throws IncompleteResultException if the method times out before the creation and initialization of the overlay is complete
     */
    public void startRemoteRing(Class<? extends OverlayObjectControl> controlClass, Vector<String> hostList, Vector<Integer> portList, Vector<ChordID> idList, Hashtable<String, String> properties, long timeout) throws URISyntaxException, InstantiationException, IncompleteResultException {
        Vector<String> nodeURIList = new Vector<String>();
        for (String address : hostList) {
            for (Integer port : portList) {
                nodeURIList.add("//" + address + ":" + Integer.toString(port));
            }
        }
        MultiVector<String, Hashtable<String, String>> nodeDescriptors = new MultiVector<String, Hashtable<String, String>>();
        for (String node : nodeURIList) {
            nodeDescriptors.add(node, properties);
        }
        startRemoteRing(controlClass, nodeDescriptors, idList, timeout);
    }

    /**
     * This method allows invoking a method remotely on an overlay object, given its URI
     * @param nodeURI the URI of the overlay object (host:port) on which the method should be invoked
     * @param method the name of the method to invoke, which must correspond to a valid signature within the interface of the overlay object type
     * @param parameters the parameters corresponding to the method, which must be serializable to be sent over the network and correspond to a valid signature within the interface of the overlay object type
     * @return the return value of the invoked method, if the method invocation was successful
     * @throws IncompleteResultException if the invocation of the method was not successful, either because an exception was thrown by the method at the remote object, the method invocation was not valid, or a timeout ocurred
     * @throws NetworkException if the failure of the invocation was due recognizably to a network communication problem between the control interface and control server at the remote host
     */
    public Object remoteControl(String nodeURI, String method, Serializable... parameters) throws IncompleteResultException, NetworkException {
        Object ret = null;
        if (!nodeURI.equals(localNodeURI)) {
            RequestVariable<Object> request = new RequestVariable<Object>();
            BigInteger requestRef;
            synchronized (executeRequests) {
                requestRef = executeRequests.randomPut(request, requestKeyGenerator);
            }
            OverlayControlMessage executeMessage = new OverlayControlMessage(requestRef, method, parameters);
            MessagingClient client = remoteClients.get(nodeURI);
            if (client != null) {
                client.send(executeMessage.getMessageBytes());
                if (request.awaitUninterruptibly(300000, TimeUnit.MILLISECONDS)) {
                    ret = request.getResult();
                    if (ret != null) {
                        if (ret instanceof Exception) {
                            throw new IncompleteResultException(null, (Exception) ret);
                        }
                    }
                }
                else {
                    throw new IncompleteResultException(null, "Timeout");
                }
            }
            else {
                Debug.println("For node " + nodeURI);
                Debug.println(remoteClients.keySet());
                throw new IllegalArgumentException("OverlayControlInterface: Cannot complete remote execution: Given node is not part of the overlay");
            }
        }
        else {
            Debug.println("Skipping remote control execution for local node");
        }
        return ret;
    }

    /**
     * Non-blocking call that allows invoking a method remotely on an overlay object, given its URI.
     * Should be used when the method has no return object, or the return value isn't needed.
     * @param nodeURI the URI of the overlay object (host:port) on which the method should be invoked
     * @param method the name of the method to invoke, which must correspond to a valid signature within the interface of the overlay object type
     * @param parameters the parameters corresponding to the method, which must be serializable to be sent over the network and correspond to a valid signature within the interface of the overlay object type
     * @throws NetworkException if the failure of the invocation was due recognizably to a network communication problem between the control interface and control server at the remote host
     */
    public void asyncRemoteControl(String nodeURI, String method, Serializable... parameters) throws NetworkException {
        if (!nodeURI.equals(localNodeURI)) {
            BigInteger requestRef = BigInteger.ZERO;
            OverlayControlMessage executeMessage = new OverlayControlMessage(requestRef, method, parameters);
            MessagingClient client = remoteClients.get(nodeURI);
            if (client != null) {
                client.send(executeMessage.getMessageBytes());
            }
            else {
                Debug.println("For node " + nodeURI);
                Debug.println(remoteClients.keySet());
                throw new IllegalArgumentException("OverlayControlInterface: Cannot complete remote execution: Given node is not part of the overlay");
            }
        }
        else {
            Debug.println("Skipping remote control execution for local node");
        }
    }

    /**
     * Non-blocking call that allows invoking a method remotely on an overlay object, given its URI,
     * specifying a return handler that will be notified when the return value of the method is available.
     * @param nodeURI the URI of the overlay object (host:port) on which the method should be invoked
     * @param returnHandler the object that will be notified of the method's return value when it is available after a successful invocation
     * @param method the name of the method to invoke, which must correspond to a valid signature within the interface of the overlay object type
     * @param parameters the parameters corresponding to the method, which must be serializable to be sent over the network and correspond to a valid signature within the interface of the overlay object type
     * @throws NetworkException if the failure of the invocation was due recognizably to a network communication problem between the control interface and control server at the remote host
     */
    public void asyncRemoteControl(String nodeURI, ReturnObjectHandler returnHandler, String method, Serializable... parameters) throws NetworkException {
        if (!nodeURI.equals(localNodeURI)) {
            BigInteger requestRef;
            synchronized (asyncExecuteRequests) {
                requestRef = (returnHandler != null) ?
                             asyncExecuteRequests.randomPut(returnHandler, requestKeyGenerator)
                           : BigInteger.ZERO;
            }
            OverlayControlMessage executeMessage = new OverlayControlMessage(requestRef, method, parameters);
            MessagingClient client = remoteClients.get(nodeURI);
            if (client != null) {
                client.send(executeMessage.getMessageBytes());
            }
            else {
                Debug.println("For node " + nodeURI);
                Debug.println(remoteClients.keySet());
                throw new IllegalArgumentException("OverlayControlInterface: Cannot complete remote execution: Given node is not part of the overlay");
            }
        }
        else {
            Debug.println("Skipping remote control execution for local node");
        }
    }

    /**
     * Sends a terminate message to the control server for the overlay object at the given URI, which
     * will result in the termination of the connection with this object via the server, as well as the
     * invocation of the object's termination procedure in the overlay if one has been defined.
     * @param nodeURI the URI of the overlay object to terminate
     * @throws NetworkException if the termination message cannot be sent due to a network issue
     */
    public void terminateInstance(String nodeURI) throws NetworkException {
        OverlayControlMessage terminateMessage = new OverlayControlMessage();
        MessagingClient client = remoteClients.get(nodeURI);
        if (client != null) {
            client.send(terminateMessage.getMessageBytes());
        }
        else {
            throw new IllegalArgumentException("OverlayControlInterface: Cannot terminate instance: Given node is not part of the overlay");
        }
    }

    /**
     * Sends a terminate message to the control server for all overlay objects, which
     * will result in the termination of the connections with those objects via the server. The resulting
     * termination of all objects may lead to an ungraceful or undetermined finalization of the overlay.
     * @throws NetworkException if the termination messages cannot be sent due to a network issue
     */
    public void terminateOverlay() throws NetworkException {
        for (String node : remoteClients.keySet()) {
            terminateInstance(node);
        }
    }

    /**
     * Implementation of the ServiceObject interface, invoked when a remote host registers with the control
     * interface (the interface should be waiting for nodes)
     * @param client the client object over which the registration message is received
     */
    public synchronized void newClient(PluggableClient client) {
        if (nodesJoined != null) {
            try {
                System.out.println("New client");
                byte[] registrationBytes = ((MessagingClient) client).receiveBytes();
                System.out.println(new String(registrationBytes));
                OverlayControlMessage registrationMessage = new OverlayControlMessage(registrationBytes);
                nodesJoined.addResult(registrationMessage.getURI());
                System.out.println("Received node registration: " + registrationMessage.getURI());
                client.endConnection();
            }
            catch (MalformedMessageException mme) {
            }
        }
    }

    /**
     * Implementation of the MessageArrivedListener interface invoked for control messages
     */
    public synchronized void signalEvent(MessageArrivedEvent event) {
        try {
            OverlayControlMessage message = new OverlayControlMessage(event.getContentBytes());
            if (message.isURIMessage()) {
                String initStatus = message.getURIStatus();
                if (!initStatus.startsWith("Failed")) {
                    if (nodesJoined != null) {
                        System.out.println("Signal joined " + message.getURI());
                        nodesJoined.addResult(message.getURI());
                    }
                    else {
                        throw new IllegalStateException("nodesJoined variable not initialized");
                    }
                }
                else {
                    try {
                        terminateInstance(message.getURI());
                    }
                    catch (NetworkException ne) {
                    }
                }
            }
            else if (message.isResultMessage()) {
                BigInteger requestRef = message.getRequestReference();
                boolean signaled = false;
                synchronized (executeRequests) {
                    RequestVariable<Object> request = executeRequests.get(requestRef);
                    if (request != null) {
                        if (message.hasResult()) {
                            request.setResult(message.getResult());
                        }
                        else {
                            request.setResult(null);
                        }
                        executeRequests.remove(requestRef);
                        signaled = true;
                    }
                }
                if (!signaled) {
                    synchronized (asyncExecuteRequests) {
                        ReturnObjectHandler returnHandler = asyncExecuteRequests.get(requestRef);
                        if (returnHandler != null) {
                            if (message.hasResult()) {
                                Object result = message.getResult();
                                if (!(result instanceof Exception)) {
                                    returnHandler.setReturnObject(result);
                                }
                                else {
                                    returnHandler.signalException((Exception) result);
                                }
                            }
                            else {
                                returnHandler.setReturnObject(null);
                            }
                            asyncExecuteRequests.remove(requestRef);
                            signaled = true;
                        }
                    }
                }
                if (!signaled) {
                    Debug.println("OverlayControlInterface: Unhandled reply", "overlayControl");
                }
            }
        }
        catch (MalformedMessageException mme) {
            Debug.printStackTrace(mme, "control");
        }
    }

    /**
     * @return the URIs of all overlay objects that have joined the overlay after a call to
     * startAsBootstrap or startRemoteRing, including the local object instance (if any)
     */
    public Vector<String> getNodeURIList() {
        Vector<String> ret = new Vector<String>(remoteClients.keySet());
        if (localNodeURI != null) {
            ret.add(localNodeURI);
        }
        if (ret.size() > 0) {
            return ret;
        }
        else {
            throw new IllegalStateException("OverlayControlInterface: Node URI list not set");
        }
    }

    /**
     * @return the URIs of all remote overlay objects that have joined the overlay after a call to
     * startAsBootstrap or startRemoteRing (excludes the local object instance if it exists)
     */
    public Vector<String> getRemoteNodeURIList() {
        return new Vector<String>(remoteClients.keySet());
//        if (ret.size() > 0) {
//            return ret;
//        }
//        else {
//            throw new IllegalStateException("OverlayControlInterface: Node URI list not set");
//        }
    }

	/**
	 * Creates a Messaging Client depending on the value of
	 * chord.NETWORK_CLASS, the default is TCP.
	 */
	private MessagingClient getClient(){
		MessagingClient initClient;
		String nc = System.getProperty("chord.NETWORK_CLASS", "TCP");
		if(nc.equals("SSL")){
			initClient = new SSLClient();
			System.out.println("getClient: Creating a SSL client");
		}
		else{
			System.out.println("getClient: Creating a TCP client");
			initClient = new TCPClient();
		}
		return initClient;
	}

}
