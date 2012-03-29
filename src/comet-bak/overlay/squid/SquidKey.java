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
 * SquidKey.java
 *
 * Created on May 11, 2007, 3:03 PM
 */

package tassl.automate.overlay.squid;

import java.math.BigInteger;
import tassl.automate.overlay.OverlayID;
import tassl.automate.overlay.chord.ChordID;
import tassl.automate.overlay.chord.ChordIDCluster;

/**
 * Concrete implementation of an {@link OverlayID} for the Squid overlay service.
 * Represents values or ranges of each of the dimensions of a multidimensional space.
 * The number of dimensions of the space is set when the object is created, depending
 * on the keyspace to which the key belongs, and does not change during its lifetime.
 * When used for indexing on a particular instance of a Squid overlay, the number of
 * dimensions of the key should match the number of dimensions expected by the overlay.
 * @author ahernandez
 */
public class SquidKey extends OverlayID {

    private String keyspace = "";
    private int spaceDimensions;
    private KeyTypes[] type;

    private String locality = ChordID.LOCAL_CLUSTER;
    String[][] keys;
    private int bits;
    private boolean hasRanges = false;

    private ChordIDCluster chordMapping = null;

    public enum KeyTypes {NUMERIC, ALPHABETIC};

    /**
     * Creates a squid key using the definition of the default keyspace. This definition must be provided by a
     * properties file.
     */
    public SquidKey(int myDimensions, int myBitLength, KeyTypes... myTypes) {
        spaceDimensions = myDimensions;
        type = new KeyTypes[spaceDimensions];
        bits = myBitLength;
        type = myTypes;
        keys = new String[spaceDimensions][];
    }

    /**
     * Creates a squid key using the definition of the given keyspace. This definition must be provided by a
     * properties file.
     */
    public SquidKey(String keyspace, int myDimensions, int myBitLength, KeyTypes... myTypes) {
        this.keyspace = keyspace;
        spaceDimensions = myDimensions;
        type = myTypes;
        bits = myBitLength;
        keys = new String[spaceDimensions][];
    }

    /**
     * Creates a squid key in the same keyspace as the key given.
     */
    public SquidKey(SquidKey master) {
        this.keyspace = master.getKeyspace();
        this.spaceDimensions = master.getSpaceDimensions();
        this.type = new KeyTypes[spaceDimensions];
        this.bits = master.getBitLength();
        this.hasRanges = master.hasRanges;
        this.chordMapping = master.chordMapping;
        keys = new String[spaceDimensions][];
        for (int i = 0; i < spaceDimensions; i++) {
            this.type[i] = master.getType(i);
            if (master.keys[i] != null) {
                keys[i] = master.keys[i];
            }
        }
    }

    public void setLocality(String myLocality) {
        locality = myLocality;
    }

    /**
     * Sets a single value for the given dimension.
     * @param keyValue this string is interpreted as a number or string depending on the type of the corresponding dimension, determined in the keyspace definition
     */
    public void setKey(int dimension, String keyValue) {
        keys[dimension] = new String[1];
        keys[dimension][0] = keyValue;
    }

    /**
     * Sets a range or ranges of values for the given dimension.
     * @param keyRanges the start and end values of each range must be defined as consecutive array elements, such that even elements represent starting values and odd elements ending values. Each string is interpreted as a number or string depending on the type of the corresponding dimension, determined in the keyspace definition.
     */
    public void setKey(int dimension, String[] keyRanges) {
        if (keyRanges.length % 2 == 0) {
            keys[dimension] = new String[keyRanges.length];
            for (int i = 0; i < keyRanges.length; i++) {
                keys[dimension][i] = keyRanges[i];
            }
            hasRanges = true;
        }
        else {
            throw new IllegalArgumentException("SquidKey: Could not set key: Ranges must be an array of min, max pairs (even size)");
        }
    }

    /**
     * @return the numeric representation of all values of this key. Depending on the number of bits defined for each dimension in the keyspace, this method might scale down numeric values that are too big and use only part of alphabetic keys.
     */
    protected BigInteger[] getKeyBits() {
        BigInteger[] ret = new BigInteger[spaceDimensions];
        for (int dimension = 0; dimension < spaceDimensions; dimension++) {
            switch (type[dimension]) {
                case NUMERIC: {
                    BigInteger keyValue = new BigInteger(keys[dimension][0]);
                    // Scale down if necessary
                    if (keyValue.bitLength() > bits) {
                        int scaling = keyValue.bitLength() - bits;
                        keyValue = keyValue.shiftRight(scaling);
                    }
                    ret[dimension] = keyValue;
                    break;
                }
                case ALPHABETIC: {
                    int numChar = bits / 5;
                    String auxKey = new String(keys[dimension][0]);
                    int fitting = numChar - auxKey.length();
                    if (fitting > 0) {
                        String padChar = "a";
                        for (int j = 0; j < fitting; j++) {
                            auxKey += padChar;
                        }
                    }
                    else if (fitting < 0) {
                        auxKey = auxKey.substring(0, numChar);
                    }
                    char letter = (char) (Character.toLowerCase(auxKey.charAt(0)) - 'a');
                    BigInteger keyValue = new BigInteger(Integer.toString(letter));
                    for (int j = 1; j < numChar; j++) {
                        keyValue = keyValue.shiftLeft(5);
                        letter = (char) (Character.toLowerCase(auxKey.charAt(j)) - 'a');
                        keyValue = keyValue.add(new BigInteger(Integer.toString(letter)));
                    }
                    ret[dimension] = keyValue;
                    break;
                }
            }
        }
        return ret;
    }

    /**
     * @return the numeric representation of all values of this key. Depending on the number of bits defined for each dimension in the keyspace, this method might scale down numeric values that are too big and use only part of alphabetic keys.
     */
    protected BigInteger[][] getKeyRanges() {
        BigInteger[][] ret = new BigInteger[spaceDimensions][];
        for (int dimension = 0; dimension < spaceDimensions; dimension++) {
            ret[dimension] = new BigInteger[keys[dimension].length];
            switch (type[dimension]) {
                case NUMERIC: {
                    for (int i = 0; i < keys[dimension].length; i++) {
                        BigInteger keyValue = new BigInteger(keys[dimension][i]);
                        // Scale down if necessary
                        if (keyValue.bitLength() > bits) {
                            int scaling = keyValue.bitLength() - bits;
                            keyValue = keyValue.shiftRight(scaling);
                        }
                        ret[dimension][i] = keyValue;
                    }
                    break;
                }
                case ALPHABETIC: {
                    int numChar = bits / 5;
                    for (int i = 0; i < keys[dimension].length; i++) {
                        String auxKey = new String(keys[dimension][i]);
                        int fitting = numChar - auxKey.length();
                        if (fitting > 0) {
                            String padChar = (i%2 == 0) ? "a" : "z";
                            for (int j = 0; j < fitting; j++) {
                                auxKey += padChar;
                            }
                        }
                        else if (fitting < 0) {
                            auxKey = auxKey.substring(0, numChar);
                        }
                        char letter = (char) (Character.toLowerCase(auxKey.charAt(0)) - 'a');
                        BigInteger keyValue = new BigInteger(Integer.toString(letter));
                        for (int j = 1; j < numChar; j++) {
                            keyValue = keyValue.shiftLeft(5);
                            letter = (char) (Character.toLowerCase(auxKey.charAt(j)) - 'a');
                            keyValue = keyValue.add(new BigInteger(Integer.toString(letter)));
                        }
                        ret[dimension][i] = keyValue;
                    }
                }
            }
        }
        return ret;
    }

    public String toString() {
        String retValue;

        retValue = keyspace != null ? keyspace : "";
        for (int i = 0; i < spaceDimensions; i++) {
            if (keys[i] != null) {
                try {
                    if (keys[i].length == 1) {
                        retValue += keys[i][0] + "\n";
                    }
                    else {
                        for (int j = 0; j < keys[i].length; j += 2) {
                            retValue += "[" + keys[i][j] + ", " + keys[i][j+1] + "] ";
                        }
                        retValue += "\n";
                    }
                }
                catch (NullPointerException npe) {
                    npe.printStackTrace();
                }
            }
        }
        if (chordMapping != null) {
            retValue += "\n" + chordMapping + "\n";
        }
        return retValue;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public int getSpaceDimensions() {
        return spaceDimensions;
    }

    public KeyTypes getType(int dimension) {
        return type[dimension];
    }

    public String getLocality() {
        return locality;
    }

    public int getBitLength() {
        return bits;
    }

    public boolean hasRanges() {
        return hasRanges;
    }

    public int hashCode() {
        String hashString = (keyspace != null) ? keyspace : "";
        for (int i = 0; i < spaceDimensions; i++) {
            hashString = hashString.concat(type[i].name());
            for (String key : keys[i]) {
                hashString = hashString.concat(key);
            }
        }
        return hashString.hashCode();
    }

    public ChordIDCluster getChordMapping() {
        return chordMapping;
    }

    public void addToMapping(ChordID chordID) {
        if (chordMapping == null) {
            chordMapping = new ChordIDCluster(chordID.getBitLength());
        }
        chordMapping.add(chordID);
    }

    public boolean equals(SquidKey other) {
        boolean ret = true;
        if (!this.keyspace.equals(other.keyspace)) {
            ret = false;
        }
        else if (this.bits != other.bits || this.spaceDimensions != other.spaceDimensions) {
            ret = false;
        }
        else if (this.hasRanges != other.hasRanges) {
            ret = false;
        }
        else {
            for (int i = 0; i < type.length; i++) {
                if (!this.type[i].equals(other.type[i])) {
                    ret = false;
                    break;
                }
            }
            if (ret) {
                for (int i = 0; i < keys.length; i++) {
                    for (int j = 0; j < keys[i].length; j++) {
                        if (!this.keys[i][j].equals(other.keys[i][j])) {
                            ret = false;
                            break;
                        }
                    }
                }
            }
        }
        return ret;
    }

    public boolean equals(ChordID chordOther) {
        boolean ret = false;
        if (chordMapping != null) {
            if (chordMapping.contains(chordOther)) {
                ret = true;
            }
        }
        return ret;
    }

}
