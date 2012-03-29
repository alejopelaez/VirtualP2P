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
 * StaticHilbertSFC.java
 *
 * Created on August 28, 2007, 2:41 PM
 */

package tassl.automate.overlay.squid;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.Vector;
import programming5.io.FileHandler;

/**
 *
 * @author aquirozh
 */
public class StaticHilbertSFC extends HilbertSFC implements Serializable {

    private static final long serialVersionUID = 4297650414602354081L;
    
    Vector<BigInteger[]>[] coordList;
    Vector<BigInteger>[] indexList;
    
    private BigInteger axisRange;
    
    private static final BigInteger MAX_INT = new BigInteger(Integer.toString(Integer.MAX_VALUE));
    private static final String saveName = "hilbert.sfc";
    
    public StaticHilbertSFC(int numberOfDimensions, int bitsPerDimension) {
        super(numberOfDimensions, bitsPerDimension);
        BigInteger indexRange = BigInteger.ONE.shiftLeft(numberOfDimensions*bitsPerDimension);
        int multiplicity = indexRange.divide(MAX_INT).add(BigInteger.ONE).intValue();
        axisRange = BigInteger.ONE.shiftLeft(bitsPerDimension);
        coordList = new Vector[multiplicity];
        indexList = new Vector[multiplicity];
        for (int i = 0; i < multiplicity; i++) {
            coordList[i] = new Vector<BigInteger[]>();
            indexList[i] = new Vector<BigInteger>();
        }
        BigInteger i = new BigInteger("0");
        while (i.compareTo(indexRange) < 0) {
            // Fill index to coordinate mapping
            BigInteger[] coords = super.indexToCoordinates(i);
            BigInteger[] listIndex = i.divideAndRemainder(MAX_INT);
            coordList[listIndex[0].intValue()].add(listIndex[1].intValue(), coords);
            // Fill coordinate to index mapping
            BigInteger index = new BigInteger("0");
            for (int d = numberOfDimensions-1; d >= 0; d--) {
                BigInteger accum = axisRange.pow(d).multiply(coords[d]);
                index = index.add(accum);
            }
            listIndex = index.divideAndRemainder(MAX_INT);
            if (listIndex[1].intValue() > indexList[listIndex[0].intValue()].size() - 1) {
                indexList[listIndex[0].intValue()].setSize(listIndex[1].intValue() + 1);
            }
            indexList[listIndex[0].intValue()].setElementAt(i, listIndex[1].intValue());
            i = i.add(BigInteger.ONE);
        }
    }
    
    public static StaticHilbertSFC loadFromFile() throws IOException {
        FileHandler loadFile = new FileHandler(saveName, FileHandler.HandleMode.READ, FileHandler.FileType.BINARY);
        return (StaticHilbertSFC) loadFile.readObject();
    }
    
    public static StaticHilbertSFC loadFromFile(String fileName) throws IOException {
        FileHandler loadFile = new FileHandler(fileName, FileHandler.HandleMode.READ, FileHandler.FileType.BINARY);
        return (StaticHilbertSFC) loadFile.readObject();
    }
    
    public void setDimensions(int numberOfDimensions) {
        throw new IllegalAccessError("StaticHilbertSFC: Cannot change mapping after instantiation; please create a new instance");
    }
    
    public void setBits(int bitsPerDimension) {
        throw new IllegalAccessError("StaticHilbertSFC: Cannot change mapping after instantiation; please create a new instance");
    }
    
    public BigInteger[] indexToCoordinates(BigInteger index) {
        BigInteger[] listIndex = index.divideAndRemainder(MAX_INT);
        return coordList[listIndex[0].intValue()].get(listIndex[1].intValue());
    }
    
    public BigInteger coordinatesToIndex(BigInteger[] coords) {
        BigInteger index = new BigInteger("0");
        for (int d = dimensions-1; d >= 0; d--) {
            BigInteger accum = axisRange.pow(d).multiply(coords[d]);
            index = index.add(accum);
        }
        BigInteger[] listIndex = index.divideAndRemainder(MAX_INT);
        return indexList[listIndex[0].intValue()].get(listIndex[1].intValue());
    }
    
    public void save() throws IOException {
        FileHandler saveFile = new FileHandler(saveName, FileHandler.HandleMode.OVERWRITE, FileHandler.FileType.BINARY);
        saveFile.write(this);
        saveFile.close();
    }
    
    public void save(String file) throws IOException {
        FileHandler saveFile = new FileHandler(file, FileHandler.HandleMode.OVERWRITE, FileHandler.FileType.BINARY);
        saveFile.write(this);
        saveFile.close();
    }
    
}
