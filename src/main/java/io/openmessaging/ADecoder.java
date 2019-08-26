package io.openmessaging;

/**
 * Created by yanghuiwei on 2019-08-25
 */
public class ADecoder extends AbstractDecoder {

    public void decode(long[] a, int aPos, int readLen) {
        //先读第一个
        a[aPos++] = getData(getBits(7));
        for (int i = 1; i < readLen; i++, aPos++) {
            int aBitsAvailable = getBits(8);
            int signed = aBitsAvailable & 1;
            aBitsAvailable = aBitsAvailable >> 1;
            long delta = getData(aBitsAvailable);
            a[aPos] = a[aPos - 1] + (signed == 1 ? -delta : delta);
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
}
