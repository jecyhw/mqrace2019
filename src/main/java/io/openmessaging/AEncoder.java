package io.openmessaging;

import java.nio.ByteBuffer;

/**
 * Created by yanghuiwei on 2019-08-25
 */
public class AEncoder extends AbstractEncoder {
    long aLenBits = 0;

    public AEncoder(ByteBuffer buf) {
        super(buf);
    }

    public int lastABitsAvailable = 0;

    public void encode(long a) {
        int aBitsAvailable;

        aBitsAvailable = getABitsAvailable(a);
        put(aBitsAvailable - 1, Const.A_BIT_LENGTH);

        int diff = aBitsAvailable - lastABitsAvailable;
        lastABitsAvailable = aBitsAvailable;

        aLenBits += getBits(diff);

        putData(a, aBitsAvailable);
    }

    private int getBits(int diff) {
        if (diff == 0) {
            return 1;
        }
        if (diff == -1) {
            return 2;
        }
        if (diff == 1) {
            return 3;
        }
        if (diff == -2) {
            return 4;
        } else if (diff == 2) {
            return 5;
        } else if (diff == -3) {
            return 6;
        } else if (diff == 3) {
            return 7;
        } else {
            if (diff < 0) {
                diff = -diff;
            }
            diff -= 4;
            return getABitsAvailable(diff) * 2 + 8;
        }
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
            return 31;
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
