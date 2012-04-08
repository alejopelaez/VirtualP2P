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

package virtualp2p.squid;

import java.math.BigInteger;
import java.util.ArrayList;

public class HilbertSFC {

    int dimensions;
    int bits;
    int init;

    private BigInteger ones, nthbits;

    public void init(int numberOfDimensions, int bitsPerDimension) {
        dimensions = numberOfDimensions;
        bits = bitsPerDimension;
        ones = new BigInteger("1");
        ones = ones.shiftLeft(dimensions).subtract(BigInteger.ONE);
        nthbits = new BigInteger("1");
        init = 0;

        nthbits = nthbits.shiftLeft(dimensions*bits).subtract(BigInteger.ONE).divide(ones).shiftRight(1);
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

    public int pow(int num, int k){
        if(k == 0)return 1;
        if(k % 2 == 0)return pow(num,k/2) * pow(num,k/2);
        else return pow(num,k/2) * pow(num,k/2) * num;
    }

    public int root(int num, int k){
        int high = num / 2; int low = 0;
        int mid = (high + low) / 2;
        while(high != low && mid != low){
            if(pow(mid, k) == num)return mid;
            else if(pow(mid, k) > num) high = mid;
            else low = mid;
            mid = (high + low) / 2;
        }
        return mid;
    }

    /**
     * Convert a set of coordinates from the mapping space to a one dimensional index.
     * @param coords each coordinate must have no more than the number of bits defined for the SFC instance
     */
    public BigInteger coordinatesToIndex(BigInteger[] coords) {
        dimensions = coords.length;
        bits = Integer.parseInt(System.getProperties().getProperty("bitLength", "160"));
        init(dimensions,  root(bits, dimensions));
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

    public int[] indexToArray(BigInteger index){
        ArrayList<Integer> array = new ArrayList<Integer>();

        BigInteger max = BigInteger.valueOf(2147483647);
        array.add(index.mod(max).intValue());
        while(index.divide(max).compareTo(BigInteger.ZERO) != 0){
            index = index.divide(max);
            array.add(index.mod(max).intValue());
        }

        int[] ret = new int[array.size()];
        for(int i = 0; i < array.size(); ++i)
            ret[i] = array.get(i);
        return ret;
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
