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
 * ChordIDCluster.java
 *
 * Created on February 8, 2007, 3:15 PM
 */

package tassl.automate.overlay.chord;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Collections;
import java.util.Vector;

/**
 * Sorted vector of Chord ids
 * @author aquirozh
 */
public class ChordIDCluster extends Vector<ChordID> implements Serializable {
    
    private static final long serialVersionUID = 8990693821288678631L;
                                                 
    boolean ranges = false;
    ChordID start;
    boolean sorted = false;
    
//    private static final ChordID ZERO = new ChordID(BigInteger.ZERO);
    
    public ChordIDCluster(int bits) {
        super();
        start = new ChordID(BigInteger.ZERO, bits);
    }
    
    public ChordIDCluster(Collection<ChordID> items) {
        super(items);
        start = new ChordID(BigInteger.ZERO, this.elementAt(0).NUM_BITS);
    }
    
    public ChordIDCluster(boolean hasRanges) {
        super();
        ranges = hasRanges;
    }
    
    public boolean hasVisited(ChordID node) {
        return (start.equals(node));
    }
    
    public boolean hasRanges() {
        return ranges;
    }
    
    public void sort() {
        if (!sorted) {
            Collections.sort(this);
            sorted = true;
        }
    }
    
    /*
     * Eliminates the cluster elements that belong to the given reference id.
     */
    public ChordIDCluster consume(ChordID from, ChordID until) {
        ChordIDCluster ret = new ChordIDCluster(ranges);
        this.sort();
        int i = 0;
        boolean wrap;;
        do {
            wrap = false;
            for (; i < this.size(); i++) {
                if (this.elementAt(i).isBetweenIntervalRight(from, until)) {
                    if (ranges && i % 2 != 0) {
                        this.insertElementAt(from, i);
                        this.insertElementAt(from.add(BigInteger.ONE), i);
                        i++;
                    }
                    break;
                }
            }
            if (i < this.size()) {
                ret.add(this.remove(i));
                while (i < this.size()) {
                    if (this.elementAt(i).isBetweenIntervalRight(from, until)) {
                        ret.add(this.remove(i));
                    }
                    else {
                        if (ranges && i % 2 != 0) {
                            ret.add(until);
                            this.insertElementAt(until.add(BigInteger.ONE), i);
                        }
                        break;
                    }
                }
            }
            if (i == 0 && from.compareTo(until) > 0 && this.size() > 0) {
                wrap = true;
            }
        } while (wrap);
        return ret;
    }
    
    @Override
    public boolean add(ChordID id) {
        sorted = false;
        return super.add(id);
    }
    
    @Override
    public void add(int index, ChordID id) {
        sorted = false;
        super.add(index, id);
    }
    
    @Override
    public boolean addAll(int index, Collection<? extends ChordID> other) {
        sorted = false;
        return super.addAll(index, other);
    }
    
    @Override
    public boolean addAll(Collection<? extends ChordID> other) {
        sorted = false;
        return super.addAll(other);
    }

    @Override
    public void addElement(ChordID id) {
        this.add(id);
    }

    @Override
    public void insertElementAt(ChordID id, int index) {
        this.add(index, id);
    }

    @Override
    public ChordID set(int index, ChordID id) {
        sorted = false;
        return super.set(index, id);
    }

    @Override
    public void setElementAt(ChordID id, int index) {
        this.set(index, id);
    }
    
}
