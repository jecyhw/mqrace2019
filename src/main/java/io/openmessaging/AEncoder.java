package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.Random;

/**
 * Created by yanghuiwei on 2019-08-25
 */
public class AEncoder extends AbstractEncoder {

    public AEncoder(ByteBuffer buf) {
        super(buf);
    }


    public void encode(long a) {
        int aBitsAvailable;

        //符号位长度可以优化
        aBitsAvailable = getABitsAvailable(a);
        //7位长度+1位符号位
        put(aBitsAvailable, 7);
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
        int t = (int) (a >> 32);
        if (t == 0) {
            return 32 - _getABitsAvailable((int) a);
        } else {
            return 64 - _getABitsAvailable(t);
        }
    }

    private int _getABitsAvailable(int a) {
        int n;
        if (a == 0) {
            return 32;
        }
        n = 1;
        if ((a >>> 16) == 0) {
            n = n + 16;
            a = a << 16;
        }
        if ((a >>> 24) == 0) {
            n = n + 8;
            a = a << 8;
        }
        if ((a >>> 28) == 0) {
            n = n + 4;
            a = a << 4;
        }
        if ((a >>> 30) == 0) {
            n = n + 2;
            a = a << 2;
        }
        n = n - (a >>> 31);
        return n;
    }

    public boolean hasRemaining() {
        return buf.remaining() > 16;
    }

    public void clear() {
        buf.clear();
    }
}
