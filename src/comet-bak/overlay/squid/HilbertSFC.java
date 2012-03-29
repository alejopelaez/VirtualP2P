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
 * HilbertSFC.java
 *
 * Created on August 27, 2007, 4:19 PM
 */

/*
 * Based on hilbert.c code by:
 *              Doug Moore
 *              Dept. of Computational and Applied Math
 *              Rice University
 *              http://www.caam.rice.edu/~dougm
 * Date:        Wed Jul  15 1998
 * Copyright (c) 1998, Rice University
 *
 * Included Acknowledgement:
 * This implementation is based on the work of A. R. Butz ("Alternative
 * Algorithm for Hilbert's Space-Filling Curve", IEEE Trans. Comp., April,
 * 1971, pp 424-426) and its interpretation by Spencer W. Thomas, University
 * of Michigan (http://www-personal.umich.edu/~spencer/Home.html) in his widely
 * available C software.  While the implementation here differs considerably
 * from his, the first two interfaces and the style of some comments are very
 * much derived from his work.
 */

package tassl.automate.overlay.squid;

import java.math.BigInteger;

/**
 * Represents a Hilbert Space Filling Curve mapping for a space of a given dimensionality and resolution (bits per dimension).
 * Implements methods for both forward (N -> 1 dimensions) and reverse (1 -> N dimensions) mappings. Uses the {@link java.math.BigInteger}
 * class to handle large numbers.
 * @author aquirozh
 */
public class HilbertSFC {

    int dimensions;
    int bits;
    int init;

    private BigInteger ones, nthbits;
    static int counter;     //@anirudh
    /**
     * Creates a mapping for the given number of dimensions and number of bits used to represent coordinates in each dimension.
     */
    public HilbertSFC(int numberOfDimensions, int bitsPerDimension) {
        dimensions = numberOfDimensions;
        bits = bitsPerDimension;
        ones = new BigInteger("1");
        ones = ones.shiftLeft(dimensions).subtract(BigInteger.ONE);
        nthbits = new BigInteger("1");
		init = 0;

        nthbits = nthbits.shiftLeft(dimensions*bits).subtract(BigInteger.ONE).divide(ones).shiftRight(1);
    }

	public HilbertSFC(int numberOfDimensions, int bitsPerDimension, int init) {
        dimensions = numberOfDimensions;
        bits = bitsPerDimension;
        ones = new BigInteger("1");
        ones = ones.shiftLeft(dimensions).subtract(BigInteger.ONE);
        nthbits = new BigInteger("1");
		this.init = init;

        nthbits = nthbits.shiftLeft(dimensions*bits).subtract(BigInteger.ONE).divide(ones).shiftRight(1);
    }

    /**
     * Changes the dimensionality of the mapping space. Subsequent calls to the mapping functions will be based on a space of
     * these dimensions.
     */
    public void setDimensions(int dimensions) {
        this.dimensions = dimensions;
        ones = new BigInteger("1");
        ones = ones.shiftLeft(dimensions).subtract(BigInteger.ONE);
        nthbits = new BigInteger("1");

        nthbits = nthbits.shiftLeft(dimensions*bits).subtract(BigInteger.ONE).divide(ones).shiftRight(1);
    }

    public int getDimensions() {
        return dimensions;
    }

    /**
     * Changes the resolution of the mapping space. Subsequent calls to the mapping functions will be based on a space of the
     * given resolution.
     */
    public void setBits(int bits) {
        this.bits = bits;
        nthbits = new BigInteger("1");
        nthbits = nthbits.shiftLeft(dimensions*bits).subtract(BigInteger.ONE).divide(ones).shiftRight(1);
    }

    public int getBitsPerDimension() {
        return bits;
    }

    /**
     * Convert an index from one dimensional space to a set of coordinates in the multidimensional space.
     * @param index can have at most a number of bits equivalent to the number of dimensions times the bits per dimension
     */
    public BigInteger[] indexToCoordinates(BigInteger index) {
        BigInteger[] ret = new BigInteger[dimensions];
        if (index.compareTo(BigInteger.ZERO) >= 0 && index.bitLength() <= (dimensions * bits)) { // Verification
            // Initialization
            for (int d = 0; d < dimensions; d++) {
                ret[d] = new BigInteger("0");
            }
            BigInteger auxIndex = new BigInteger(index.toByteArray());
            int rotation = 0;
            BigInteger reflection = new BigInteger("0");

            // Calculation
            auxIndex = auxIndex.xor(auxIndex.shiftRight(1));
            auxIndex = auxIndex.xor(nthbits);
            for (int b = bits-1; b >= 0; b--) {
                BigInteger auxBits = auxIndex.shiftRight(dimensions*b).and(ones);
                reflection = reflection.xor(rotateRight(auxBits, dimensions-rotation, dimensions));
                for (int d = 0; d < dimensions; d++) {
                    ret[d] = ret[d].or(reflection.shiftRight(d).and(BigInteger.ONE).shiftLeft(b));
                }
                reflection = reflection.xor(BigInteger.ONE.shiftLeft(rotation));
                rotation = adjust_rotation(rotation, auxBits);
            }
        }
        else {
            throw new IllegalArgumentException("HilbertSFC: Cannot calculate coordinates: Index must be positive and have no more bits than " + dimensions * bits + " bits for this SFC instance.");
        }
        return ret;
    }

    /**
     * Convert a set of coordinates from the mapping space to a one dimensional index.
     * @param coords each coordinate must have no more than the number of bits defined for the SFC instance
     */
    public BigInteger coordinatesToIndex(BigInteger[] coords) {
        BigInteger index = new BigInteger("0");
        // Verification
        boolean argumentsPassed = true;
        for (int d = 0; d < dimensions; d++) {
            if (coords[d].compareTo(BigInteger.ZERO) < 0 || coords[d].bitLength() > bits) {
                argumentsPassed = false;
                break;
            }
			else if (init-1 == d){
				coords[d] = BigInteger.valueOf(2).pow(bits*dimensions).subtract(BigInteger.valueOf(1)).subtract(coords[d]);
			}
        }
        if (argumentsPassed) {
            // Initialization
            int rotation = 0;
            BigInteger reflection = new BigInteger("0");

            // Calculation
            for (int b = bits-1; b >= 0; b--) {
                BigInteger auxBits = new BigInteger(reflection.toByteArray());
                reflection = new BigInteger("0");
                for (int d = 0; d < dimensions; d++) {
                    reflection = reflection.or(coords[d].shiftRight(b).and(BigInteger.ONE).shiftLeft(d));
                }
                auxBits = auxBits.xor(reflection);
                auxBits = rotateRight(auxBits, rotation, dimensions);
                index = index.or(auxBits.shiftLeft(dimensions*b));
				reflection = reflection.xor(BigInteger.ONE.shiftLeft(rotation));
                rotation = adjust_rotation(rotation, auxBits);
            }
            index = index.xor(nthbits);
            for (int d = 1; ; d *= 2) {
                BigInteger t;
                if (d <= 32) {
                    t = index.shiftRight(d);
                    if (t.equals(BigInteger.ZERO)) {
                        break;
                    }
                }
                else {
                    t = index.shiftRight(32);
                    t = t.shiftRight(d - 32);
                    if (t.equals(BigInteger.ZERO)) {
                        break;
                    }
                }
                index = index.xor(t);
                //calling custom hash function
                //if (CometConstants.LoadbalancingTest)
                //index = hashIndex(index); //@anirudh
            }
        }
        else {
            throw new IllegalArgumentException("HilbertSFC: Cannot calculate index: Coordinates must be positive and have no more than " + bits + " bits for this SFC instance");
        }
        return index;

    }

     private int adjust_rotation(int rotation, BigInteger x) {
        x = x.and(x.negate().and(BigInteger.ONE.shiftLeft(dimensions-1).subtract(BigInteger.ONE)));
        while (!x.equals(BigInteger.ZERO)) {
            x = x.shiftRight(1);
            ++rotation;
        }
        if (++rotation >= dimensions) {
            rotation -= dimensions;
        }
        return rotation;
    }

    private BigInteger rotateRight(BigInteger arg, int nRots, int nDims) {
        return arg.shiftRight(nRots).or(arg.shiftLeft(nDims-nRots)).and(BigInteger.ONE.shiftLeft(nDims).subtract(BigInteger.ONE));
    }

}
