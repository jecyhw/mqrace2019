package io.openmessaging;

/**
 * Created by yanghuiwei on 2019-08-25
 */
public class ADecoder extends AbstractDecoder {

    public void decode(long[] a, int aPos, int readLen) {
        //先读第一个
        for (int i = 0; i < readLen; i++, aPos++) {
            int aBitsAvailable = getBits(Const.A_BIT_LENGTH) + 1;
            a[aPos] = getData(aBitsAvailable);
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
