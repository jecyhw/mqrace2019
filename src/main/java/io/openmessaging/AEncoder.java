package io.openmessaging;

import java.nio.ByteBuffer;

/**
 * Created by yanghuiwei on 2019-08-25
 */
public class AEncoder extends AbstractEncoder {

    public AEncoder(ByteBuffer buf) {
        super(buf);
    }

    public void encodeFirst(long a) {
        int aBitsAvailable = getABitsAvailable(a);
        put(aBitsAvailable, 7);
        putData(a, aBitsAvailable);
    }

    public void encode(long a) {
        int aBitsAvailable;
        int signFlag = 0;
        if (a < 0) {
            a = -a;
            //最后一位记录符号位
            signFlag = 1;
        }
        //符号位长度可以优化
        aBitsAvailable = getABitsAvailable(a);
        //7位长度+1位符号位
        put((aBitsAvailable << 1) | signFlag, 8);
        putData(a, aBitsAvailable);
    }

    private void putData(long a, int aBitsAvailable) {
        if (aBitsAvailable < 32) {
            put((int)a, aBitsAvailable);
        } else {
            //低31位
            int lowBit31 = ((int)a) & 0x7fffffff;
            put(lowBit31, 31);
            putData(a >>> 31, aBitsAvailable - 31);
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
