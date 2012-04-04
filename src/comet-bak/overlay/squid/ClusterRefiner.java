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
 * ClusterRefiner.java
 *
 * Created on October 2, 2007, 3:39 PM
 */

package tassl.automate.overlay.squid;

import java.math.BigInteger;
import programming5.arrays.TupleGenerator;
import programming5.collections.MultiVector;

/**
 *
 * @author aquirozh
 */
public class ClusterRefiner {
    
    HilbertSFC mapping;
    int dimensions;
    int dimBits;
    int indexBits;
    
    protected MultiVector<BigInteger, BigInteger[]> result;
    protected int refinement;
        
    public ClusterRefiner(HilbertSFC masterMapping) {
        mapping = new HilbertSFC(masterMapping.getDimensions(), 1);
        dimensions = masterMapping.getDimensions();
        dimBits = masterMapping.getBitsPerDimension();
        indexBits = dimensions * dimBits;
    }
    
    public void refine(BigInteger[] rangeIn, int refineTo) {
        result = new MultiVector<BigInteger, BigInteger[]>();
        if (refineTo <= dimBits) {
            refinement = refineTo; //advanceRefinement(rangeIn, refineTo);   // Returns greatest bit index among dimensions up to and including first different bit between each dimension range, starting from refineTo parameter
            mapping.setBits(refinement);
            BigInteger[] newRange = getSignificantBits(rangeIn, refinement);
            int[] sizes = new int[dimensions];
            for (int i = 0; i < dimensions; i++) {
                sizes[i] = 2;
            }
            BigInteger[] coordinates = new BigInteger[dimensions];
            TupleGenerator tg = new TupleGenerator(sizes);
            int[] coordIndex = tg.nextTuple();
            while (coordIndex != null) {
                BigInteger[] rangeOut = new BigInteger[2*dimensions];
                for (int i = 0; i < dimensions; i++) {
                    coordinates[i] = newRange[2*i + coordIndex[i]];
                    if (coordIndex[i] == 0) {
                        rangeOut[2*i] = rangeIn[2*i];
                        rangeOut[2*i+1] = newRange[2*i].add(BigInteger.ONE).shiftLeft(dimBits-refinement).subtract(BigInteger.ONE);
                        if (rangeIn[2*i+1].compareTo(rangeOut[2*i+1]) < 0) {
                            rangeOut[2*i+1] = rangeIn[2*i+1];
                        }
                    } 
                    else {
                        rangeOut[2*i] = newRange[2*i+1].shiftLeft(dimBits-refinement);
                        if (rangeOut[2*i].compareTo(rangeIn[2*i]) < 0) {
                            rangeOut[2*i] = rangeIn[2*i];
                        }
                        rangeOut[2*i+1] = rangeIn[2*i+1];
                    }
                }
                BigInteger index = mapping.coordinatesToIndex(coordinates);
                BigInteger resultIndex = index.shiftLeft(indexBits - dimensions*refinement);
                BigInteger[] currentRange;
                if ((currentRange = result.getInSecond(resultIndex)) == null) {
                    result.add(resultIndex, rangeOut);
                }
                coordIndex = tg.nextTuple();
            }
        } 
        else {
            throw new IllegalArgumentException("ClusterRefiner: Cannot refine further: Already at or past bit length");
        }
    }
    
    public int getDivisionSize() {
        return result.size();
    }
    
    public BigInteger[] getRange(int index) {
        return result.getInSecondAt(index);
    }
    
    public int currentRefinement() {
        return refinement;
    }
    
    public BigInteger getIndex(int index) {
        return result.getInFirstAt(index);
    }
    
    private int advanceRefinement(BigInteger[] range, int start) {
        int max = start;
        for (int i = 0; i < dimensions; i++) {
            int difference = dimBits - range[2*i].xor(range[2*i+1]).bitLength() + 1;
            if (difference > start) {
                if (difference <= dimBits && difference > max) {
                    max = difference;
                }
            }
            else {
                max = start;
                break;
            }
        }
        if (max > dimBits) {
            max = dimBits;
        }
        return max;
    }
    
    private BigInteger[] getSignificantBits(BigInteger[] range, int numBits) {
        BigInteger[] ret = new BigInteger[range.length];
        BigInteger mask = BigInteger.ONE.shiftLeft(dimBits-numBits).subtract(BigInteger.ONE);
        for (int i = 0; i < range.length; i++) {
            ret[i] = range[i].andNot(mask).shiftRight(dimBits-numBits);
        }
        return ret;
    }
    
    private BigInteger[] tighterRange(BigInteger[] r1, BigInteger[] r2) {
        BigInteger[] ret = new BigInteger[2*dimensions];
        for (int i = 0; i < r1.length; i += 2) {
            ret[i] = (r1[i].compareTo(r2[i]) > 0) ? r1[i] : r2[i];
            ret[i+1] = (r1[i+1].compareTo(r2[i+1]) < 0) ? r1[i+1] : r2[i+1];
        }
        return ret;
    }
    
}