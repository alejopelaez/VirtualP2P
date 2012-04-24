package virtualp2p.squid

import java.math.BigInteger

/**
 * User: alejandro
 * Date: 3/04/12
 * Time: 04:01 PM
 */

/**
 * Class that represents the id of a squid node
 * It is a multi-dimensional key
 * @param dimensions The number of dimension
 * @param bitLength The length of each dimension
 * @param keyTypes  The types of the dimensions
 */

class SquidId(val dimensions: Int, val bitLength: Int, val keyTypes: Array[String], var keyData : Array[(String, String)]) {

  object Types extends Enumeration {
    type Types = Val
    val Numeric = "numeric"
    val Alphabetic = "alphabetic"
  }

  //var keySpace : String
  val spaceDimensions: Int = dimensions
  val types: Array[String] = keyTypes
  //var locality : String = ChordID.LOCAL_CLUSTER;
  var keys: Array[(String, String)] = keyData

  def root(num : Int, k : Int) : Int = {
    def pow(num : Int, k : Int) : Int = {
      if (k == 0) 1;
      else if (k%2 == 0) pow(num, k/2) * pow(num, k/2);
      else pow(num, k/2) * pow(num, k/2) * num;
    }

    var high = num; var low = 0; var mid = (high + low) / 2;
    while(high != low && mid != low){
      if (pow(mid, k) == num)return mid;
      else if (pow(mid, k) > num)high = mid;
      else low = mid;
      mid = (high + low) / 2;
    }
    mid;
  }
  val bits: Int = root(bitLength, dimensions)

  var hasRanges: Boolean = false
  //var ChordIDCluster chordMapping : ChordIDCluster = null;

  //def this(dimensions: Int, bitLength: Int, keyTypes: Array[String]) = this(dimensions, bitLength, keyTypes, new Array[(String, String)])

  /**
   * Sets a single value for the given dimension.
   * @param dimension The dimension to set the value into.
   * @param value This string is interpreted as a number or string depending on the type of the corresponding
   *              dimension, determined in the keySpace definition.
   */
  def setKey(dimension: Int, value: String) {
    keys(dimension) = (value, value);
  }

  /**
   * Sets a range of values for the given dimension.
   * @param keyRange The range must be a consecutive range of elements.
   */
  def setKey(dimension: Int, keyRange: (String, String)) {
    keys(dimension) = keyRange
    hasRanges = true
  }

  /**
   * @return the numeric representation of all values of this key. Depending on the number of bits defined for each
   *         dimension in the keySpace, this method might scale down numeric values that are too big and use only part of
   *         alphabetic keys.
   */
  def getKeyBits: Array[BigInt] = {
    var ret = new Array[BigInt](spaceDimensions);
    for (dimension <- 0 to spaceDimensions - 1) {
      types(dimension) match {
        case Types.Numeric => {
          var keyValue = new BigInteger(keys(dimension)._1);
          // Scale down if necessary
          if (keyValue.bitLength > bits) {
            var scaling = keyValue.bitLength - bits;
            keyValue = keyValue.shiftRight(scaling);
          }
          ret(dimension) = new BigInt(keyValue);
        }
        case Types.Alphabetic => {
          var numChar = bits / 4;
          var auxKey = new String(keys(dimension)._1);
          var fitting = numChar - auxKey.length();
          if (fitting > 0) auxKey += "a" * fitting
          else if (fitting < 0) {
            auxKey = auxKey.substring(0, numChar);
          }
          var letter: Int = (Character.toLowerCase(auxKey.charAt(0)) - 'a');
          var keyValue = new BigInteger(Integer.toString(letter));
          for (j <- 1 to numChar - 1) {
            keyValue = keyValue.shiftLeft(4);
            letter = (Character.toLowerCase(auxKey.charAt(j)) - 'a');
            keyValue = keyValue.add(new BigInteger(Integer.toString(letter)));
          }
          ret(dimension) = new BigInt(keyValue);
        }
      }
    }
    ret;
  }

  /**
   * @return the numeric representation of all values of this key. Depending on the number of bits defined for each
   *         dimension in the keySpace, this method might scale down numeric values that are too big and use only part of
   *         alphabetic keys.
   */
  def getKeys: Array[(BigInt, BigInt)] = {
    var ret = new Array[(BigInt, BigInt)](spaceDimensions);
    for (dimension <- 0 to spaceDimensions - 1) {
      types(dimension) match {
        case Types.Numeric => {
          var keyValue1: BigInteger = new BigInteger(keys(dimension)._1);
          var keyValue2: BigInteger = new BigInteger(keys(dimension)._2);
          // Scale down if necessary
          if (keyValue1.bitLength() > bits) {
            var scaling = keyValue1.bitLength - bits;
            keyValue1 = keyValue1.shiftRight(scaling);
          }
          if (keyValue2.bitLength() > bits) {
            var scaling = keyValue2.bitLength - bits;
            keyValue2 = keyValue2.shiftRight(scaling);
          }
          ret(dimension) = (new BigInt(keyValue1), new BigInt(keyValue2));
        }
        case Types.Alphabetic => {
          var numChar = bits / 5;
          var auxKey1 = new String(keys(dimension)._1);
          var auxKey2 = new String(keys(dimension)._2);
          var fitting = numChar - auxKey1.length();
          if (fitting > 0) {
            var padChar = "a";
            for (j <- 0 to fitting - 1)
              auxKey1 += padChar;
          }
          else if (fitting < 0) {
            auxKey1 = auxKey1.substring(0, numChar);
          }

          fitting = numChar - auxKey2.length();
          if (fitting > 0) {
            var padChar = "a";
            for (j <- 0 to fitting - 1)
              auxKey2 += padChar;
          }
          else if (fitting < 0) {
            auxKey2 = auxKey2.substring(0, numChar);
          }
          var letter = (Character.toLowerCase(auxKey1.charAt(0)) - 'a');
          var keyValue1: BigInteger = new BigInteger(Integer.toString(letter));
          letter = (Character.toLowerCase(auxKey2.charAt(0)) - 'a');
          var keyValue2: BigInteger = new BigInteger(Integer.toString(letter));
          for (j <- 1 to numChar - 1) {
            keyValue1 = keyValue1.shiftLeft(5);
            letter = (Character.toLowerCase(auxKey1.charAt(j)) - 'a');
            keyValue1 = keyValue1.add(new BigInteger(Integer.toString(letter)));
            keyValue2 = keyValue2.shiftLeft(5);
            letter = (Character.toLowerCase(auxKey2.charAt(j)) - 'a');
            keyValue2 = keyValue2.add(new BigInteger(Integer.toString(letter)));
          }
          ret(dimension) = (new BigInt(keyValue1), new BigInt(keyValue2));
        }
      }
    }
    ret;
  }

  override def toString: String = {
    var retValue: String = "";

    for (i <- 0 to spaceDimensions - 1) {
      if (keys(i) != null) {
        try {
          if (keys(i)._1 == keys(i)._2) {
            retValue += keys(i)._1 + "\n";
          }
          else {
            retValue += "[" + keys(i)._1 + ", " + keys(i)._2 + "] " + "\n";
          }
        }
        catch {
          case e: NullPointerException => {
            e.printStackTrace();
          }
        }
      }
    }
    retValue;
  }

  def getType(dimension: Int): String = {
    types(dimension);
  }

  override def hashCode: Int = {
    var hashString: String = "";
    for (i <- 0 to spaceDimensions - 1) {
      hashString = hashString.concat(types(i));
      hashString = hashString.concat(keys(i)._1 + keys(i)._2)
    }
    hashString.hashCode();
  }
}
