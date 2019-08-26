package io.openmessaging;

import java.nio.ByteBuffer;

/**
 * Created by yanghuiwei on 2019-08-25
 */
public class AEncoder extends AbstractEncoder {

    public AEncoder(ByteBuffer buf) {
        super(buf);
    }
    public void encode(long a) {
        int bitsAvailable;
        int signFlag = 0;
        if (a < 0) {
            a = -a;
            //最后一位记录符号位
            signFlag = 1;
        }
        //符号位长度可以优化
        bitsAvailable = getABitsAvailable(a);
        //7位长度+1位符号位
        put((bitsAvailable << 1) | signFlag, 8);

        if (bitsAvailable <= 32) {
            put((int)a, bitsAvailable);
        } else {
            int highBit32 = (int) (a >>> 32);
            //先放高位，再放低位
            put(highBit32, bitsAvailable - 32);
            put((int)a, 32);
        }
    }

    private int getABitsAvailable(long a) {
        if (a == 0) {
            return 1;
        }
        int cnt = 0;
        while (a > 0) {
            cnt++;
            a >>= 1;
        }
        return cnt;
    }

    public boolean hasRemaining() {
        return buf.remaining() > 8;
    }

    public void clear() {
        buf.clear();
    }
}
