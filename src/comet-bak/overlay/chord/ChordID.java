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

package tassl.automate.overlay.chord;

import java.math.BigInteger;
import java.util.Random;
import programming5.io.Debug;
import programming5.net.MalformedMessageException;
import programming5.net.Message;

import tassl.automate.overlay.OverlayID;

/**
 * Concrete implementation of an {@link OverlayID} for the Chord overlay service.
 * Uses a BigInteger object to hold a numeric identifier. The number of bits used to represent 
 * the identifier can be set by defining the chord.ID_BITS property in the chord properties 
 * file; if the property is not set, the default value is 160.
 * @author ahernandez
 */
public class ChordID extends OverlayID implements Comparable {

    private static final long serialVersionUID = 5374582933625086906l;
    protected BigInteger value;
    protected String cluster;
    public static final String LOCAL_CLUSTER = "this";
    public static final String ANY_CLUSTER = "any";
    public static final String ALL_CLUSTERS = "all";
    protected static String REFERENCE_CLUSTER;
    private static Random randomGenerator = new Random(System.currentTimeMillis());

    static {
        REFERENCE_CLUSTER = System.getProperty("chord.LOCAL_CLUSTER", LOCAL_CLUSTER);
    }
    protected final int NUM_BITS;
    protected final BigInteger RING_DIMENSION;

    /**
     * Default constructor: creates an id object with a random value, using the specified number of bits.
     */
    public ChordID(int bits) {
        Debug.println("Chord bits: " + bits, "chord");
        NUM_BITS = bits;
        RING_DIMENSION = new BigInteger("2").pow(NUM_BITS);
        value = new BigInteger(NUM_BITS, randomGenerator);
        cluster = REFERENCE_CLUSTER;
    }

    /**
     * Creates an id object with the given value, which must have at most the specified number of bits.
     * @param fixedValue numeric value for the identifier, which can be obtained externally by hashing or
     * otherwise.
     */
    public ChordID(BigInteger fixedValue, int bits) {
        NUM_BITS = bits;
        if (fixedValue.bitLength() <= NUM_BITS) {
            value = fixedValue;
        }
        else {
            throw new IllegalArgumentException("ChordID: Cannot create: Given value (" + fixedValue.toString() + ":" + fixedValue.bitLength() + ") exceeds the bit size (" + NUM_BITS + ")");
        }
        RING_DIMENSION = new BigInteger("2").pow(NUM_BITS);
        cluster = REFERENCE_CLUSTER;
    }

    /**
     * Creates an id object from a given string representation (using the syntax returned by the toString
     * method
     * @throws IllegalArgumentException if a string with incorrect syntax is given
     */
    public ChordID(String stringRep) throws IllegalArgumentException {
        try {
            Message idMsg = new Message(stringRep);
            value = new BigInteger(idMsg.getMessageItem(0));
            NUM_BITS = idMsg.getItemAsInt(1);
            if (value.bitLength() > NUM_BITS) {
                throw new IllegalArgumentException("ChordID: Cannot create from string: Given value (" + value.toString() + ":" + value.bitLength() + ") exceeds the bit size (" + NUM_BITS + ")");
            }
            RING_DIMENSION = new BigInteger("2").pow(NUM_BITS);
            cluster = REFERENCE_CLUSTER;
        }
        catch (MalformedMessageException mme) {
            throw new IllegalArgumentException("ChordID: Cannot create from string representation: " + mme.getMessage());
        }
    }

    public void setCluster(String clusterID) {
        cluster = clusterID;
    }

    public String getCluster() {
        return cluster;
    }

    /**
     * Determines if this ChordID belongs to the local cluster
     * @param localClusterID real name of the local cluster
     * @return true if this object's cluster ID is set to the local cluster or equals the given
     * cluster ID
     */
    public boolean isLocal() {
        return (cluster.equals(LOCAL_CLUSTER) || cluster.equals(REFERENCE_CLUSTER) || cluster.equals(ALL_CLUSTERS) || cluster.equals(ANY_CLUSTER));
    }

    public boolean belongsTo(String clusterID) {
        return cluster.equals(clusterID);
    }

    /**
     * Determines if this id belongs to all clusters
     * @return true if this object's cluster ID is set to all clusters
     */
    public boolean isOfAllClusters() {
        return cluster.equals(ALL_CLUSTERS);
    }

    /**
     * Determines if this id belongs to any cluster
     * @return true if this object's cluster ID is set to any cluster
     */
    public boolean isOfAnyCluster() {
        return cluster.equals(ANY_CLUSTER);
    }

    /**
     * Implementation of the comparable interface, to allow these objects to be compared and sorted
     * @param cid2 ChordID with which to compare
     * @return numeric difference between the ID values, or 0 if equal
     */
    public int compareTo(Object cid2) {
        int ret = 0;
        if (cid2 instanceof ChordID) {
            ret = this.value.compareTo(((ChordID) cid2).value);
        }
        return ret;
    }

    /**
     * @return true if other is ChordID with same value, false otherwise
     */
    public boolean equals(Object other) {
        boolean ret = false;
        if (other instanceof ChordID) {
            ret = this.compareTo((ChordID) other) == 0;
        }
        return ret;
    }

    /**
     * Determines if the current object's value is exclusively between the values of the given ids. Since
     * the ID space in chord is circular, the lower limit can be greater than the upper limit, in which
     * case the interval jumps from the maximum id value to the minimum id value.
     * @param a the lower limit, clockwise
     * @param b the upper limit, clockwise
     * @return true if current id belongs to (a, b)
     */
    public boolean isBetween(ChordID a, ChordID b) {
        boolean ret = false;
        if (a.lessThan(b)) {
            if (a.lessThan(this) && this.lessThan(b)) {
                ret = true;
            }
        }
        else {
            if (a.lessThan(this) || this.lessThan(b)) {
                ret = true;
            }
        }
        return ret;
    }

    /**
     * Determines if the current object's value is between the values of the given ids, including the
     * upper limit. Since the ID space in chord is circular, the lower limit can be greater than the
     * upper limit, in which case the interval jumps from the maximum id value to the minimum id value.
     * @param a the lower limit, clockwise
     * @param b the upper limit, clockwise
     * @return true if current id belongs to (a, b]
     */
    public boolean isBetweenIntervalRight(ChordID a, ChordID b) {
        boolean ret = false;
        if (a.lessThan(b)) {
            if (a.lessThan(this) && this.lessThanOrEqual(b)) {
                ret = true;
            }
        }
        else {
            if (a.lessThan(this) || this.lessThanOrEqual(b)) {
                ret = true;
            }
        }
        return ret;
    }

    /**
     * Determines if the current object's value is between the values of the given ids, including the
     * lower limit. Since the ID space in chord is circular, the lower limit can be greater than the
     * upper limit, in which case the interval jumps from the maximum id value to the minimum id value.
     * @param a the lower limit, clockwise
     * @param b the upper limit, clockwise
     * @return true if current id belongs to [a, b)
     */
    public boolean isBetweenIntervalLeft(ChordID a, ChordID b) {
        boolean ret = false;
        if (a.lessThan(b)) {
            if (a.lessThanOrEqual(this) && this.lessThan(b)) {
                ret = true;
            }
        }
        else {
            if (a.lessThanOrEqual(this) || this.lessThan(b)) {
                ret = true;
            }
        }
        return ret;
    }

    /**
     * Returns an id that is greater than the current object's value by the given amount, wrapping around
     * at the maximum value.
     */
    public ChordID add(BigInteger amount) {
        return new ChordID(value.add(amount).mod(RING_DIMENSION), NUM_BITS);
    }

    public int getBitLength() {
        return NUM_BITS;
    }

    /**
     * The ring dimension is the number of distinct ids that can be obtained at the given bit length (2^numBits)
     */
    public BigInteger getRingDimension() {
        return new BigInteger(RING_DIMENSION.toString());
    }

    /**
     * The ring difference is the number of ids between this id and the other id given, taking into 
     * account that the id space is circular. This means that the difference goes through 0 if this id 
     * is less than the other id.
     */
    public BigInteger ringDifference(ChordID other) {
        BigInteger ret;
        if (other.NUM_BITS == this.NUM_BITS) {
            if (other.lessThanOrEqual(this)) {
                ret = this.value.subtract(other.value);
            }
            else {
                ret = this.value.add(RING_DIMENSION.subtract(other.value));
            }
        }
        else {
            throw new IllegalArgumentException("ChordID: Cannot compare these IDs: Different number of bits");
        }
        return ret;
    }

    /**
     * @return the chord id encoded as a string with a specific syntax
     */
    @Override
    public String toString() {
        String ret = null;
        try {
            ret = Message.constructHeaderlessMessage(value.toString(), NUM_BITS).getMessage();
        }
        catch (MalformedMessageException mme) {
            mme.printStackTrace();
        }
        return ret;
    }

    @Override
    public int hashCode() {
        return value.toString().hashCode();
    }

    public BigInteger getIndexValue() {
        return new BigInteger(value.toByteArray());
    }

    /*
     * Returns an id with a value that is greater than the current object's value by 2^i, wrapping around
     * at the maximum value.
     */
    protected ChordID fingerStep(int i) {
        BigInteger newValue = value.add(new BigInteger("2").pow(i)).mod(RING_DIMENSION);
        return new ChordID(newValue, NUM_BITS);
    }

    /*
     * Utility comparison method, used in the public interval methods. Not exposed because of the circular
     * ID space.
     */
    private boolean lessThan(ChordID other) {
        return this.compareTo(other) < 0;
    }

    /*
     * Utility comparison method, used in the public interval methods. Not exposed because of the circular
     * ID space.
     */
    private boolean lessThanOrEqual(ChordID other) {
        return this.compareTo(other) <= 0;
    }
    
}
