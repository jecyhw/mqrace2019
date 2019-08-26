package io.openmessaging;

/**
 * Created by yanghuiwei on 2019-08-25
 */
public class ADecoder extends AbstractDecoder {

    public void decode(long[] a, int aPos, int readLen) {
        for (int i = 0; i < readLen; i++, aPos++) {
            int bitsAvailable = getBits(8);
            int signed = bitsAvailable & 1;
            bitsAvailable = bitsAvailable >> 1;

            long delta;
            if (bitsAvailable <= 32) {
                delta = getBits(bitsAvailable);
            } else {
                long highBit32 = getBits(bitsAvailable - 32);
                int lowBit32 = getBits(32);
                delta = (highBit32 << 32) | (lowBit32 & 0xffffffffL);
            }

            a[aPos] = a[aPos - 1] + (signed == 1 ? -delta : delta);
        }
    }
}
