package io.openmessaging;

/**
 * Created by yanghuiwei on 2019-08-25
 */
public class ADecoder extends AbstractDecoder {

    public void decode(long[] a, int aPos, int readLen) {
        int lastABitsAvailable = 0;
        //先读第一个
        for (int i = 0; i < readLen; i++, aPos++) {
            lastABitsAvailable = encodeLength() + lastABitsAvailable;
            a[aPos] = getData(lastABitsAvailable);
        }
    }

    private long getData(int aBitsAvailable) {
        if (aBitsAvailable < 32) {
            return getBits(aBitsAvailable);
        } else {
            long lowBit31 = getBits(31);
            return (getData(aBitsAvailable - 31) << 31) | lowBit31;
        }
    }

    private int encodeLength() {
        if (getBits(1) == 0) {
            return 0;
        } else if (getBits(1) == 0) {
            return  -1;
        } else if (getBits(1) == 0) {
            return  1;
        } else if (getBits(1) == 0) {
            return  -2;
        } else if (getBits(1) == 0) {
            return  2;
        } else if (getBits(1) == 0) {
            return  -3;
        } else if (getBits(1) == 0) {
            return  3;
        } else {
            return getAdaptiveDeltaOfDelta(getAdaptive());
        }
    }

    private int getAdaptiveDeltaOfDelta(int bitsValue) {
        return (bitsValue & 1) == 0 ? ((bitsValue >> 1) + 4) : -((bitsValue >> 1) + 4);
    }

    private int getAdaptive() {
        if (getBits(1) == 0) {
            return getBits(2);
        }
        else  if (getBits(1) == 0) {
            return getBits(3);
        }
        else  if (getBits(1) == 0) {
            return getBits(4);
        }
        else  if (getBits(1) == 0) {
            return getBits(5);
        }
        else  if (getBits(1) == 0) {
            return getBits(6);
        }
        else  if (getBits(1) == 0) {
            return getBits(7);
        }
        else   if (getBits(1) == 0) {
            return getBits(8);
        }
        else  if (getBits(1) == 0) {
            return getBits(9);
        }
        return 0;
    }
}
