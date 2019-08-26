package io.openmessaging;

import java.nio.ByteBuffer;

/**
 * Created by yanghuiwei on 2019-08-26
 */
public class BodyEncoder extends AbstractEncoder {
    private ByteBuffer prevBodyBuf = null;
    long byteLen = 0, shortLen = 0, intLen = 0, longLen = 0;

    public BodyEncoder() {
    }

    public void encode(byte[] body) {
        ByteBuffer bodyBuf = ByteBuffer.wrap(body);
        statShort(bodyBuf);
        if (prevBodyBuf != null) {
            statLong(bodyBuf);
        }
        prevBodyBuf = bodyBuf;
    }

    private void statLong(ByteBuffer bodyBuf) {
        bodyBuf.clear();
        prevBodyBuf.clear();
        while (bodyBuf.remaining() > 8) {
            long diff = Math.abs(bodyBuf.getLong() - prevBodyBuf.getLong());
            longLen += getABitsAvailable(diff) + 7;
        }
        intLen += Math.abs(getABitsAvailable(bodyBuf.getShort() - prevBodyBuf.getShort())) + 5;
    }

    private void statShort(ByteBuffer bodyBuf) {
        bodyBuf.clear();
        prevBodyBuf.clear();
        while (bodyBuf.remaining() > 8) {
            long diff = Math.abs(bodyBuf.getLong());
            longLen += getABitsAvailable(diff) + 7;
        }
        intLen += Math.abs(getABitsAvailable(bodyBuf.getShort())) + 5;
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

    public int getEncodeWithSigh(long val) {
        if (val == 0) {
            return 3;
        }
        val = Math.abs(val);

        int cnt = 0;
        while (val > 0) {
            cnt++;
            val >>= 1;
        }
        return cnt * 2 + 1;
    }

}
