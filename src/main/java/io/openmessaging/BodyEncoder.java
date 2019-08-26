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
        if (prevBodyBuf != null) {
            statLong(bodyBuf);
            statInt(bodyBuf);
            statShort(bodyBuf);
            statByte(bodyBuf);
        }
        prevBodyBuf = bodyBuf;
    }

    private void statLong(ByteBuffer bodyBuf) {
        bodyBuf.clear();
        prevBodyBuf.clear();
        while (bodyBuf.hasRemaining()) {
            long diff = bodyBuf.getLong() - prevBodyBuf.getLong();
            longLen += getBitsAvailable(diff);
        }
    }

    private void statInt(ByteBuffer bodyBuf) {
        bodyBuf.clear();
        prevBodyBuf.clear();
        while (bodyBuf.hasRemaining()) {
            long diff = bodyBuf.getInt() - prevBodyBuf.getInt();
            intLen += getBitsAvailable(diff);
        }
    }

    private void statShort(ByteBuffer bodyBuf) {
        bodyBuf.clear();
        prevBodyBuf.clear();
        while (bodyBuf.hasRemaining()) {
            long diff = bodyBuf.getShort() - prevBodyBuf.getShort();
            shortLen += getBitsAvailable(diff);
        }
    }

    private void statByte(ByteBuffer bodyBuf) {
        bodyBuf.clear();
        prevBodyBuf.clear();
        while (bodyBuf.hasRemaining()) {
            long diff = bodyBuf.get() - prevBodyBuf.get();
            byteLen += getBitsAvailable(diff);
        }
    }

    public int getBitsAvailable(long val) {
        if (val == 0) {
            return 1;
        }

        val = Math.abs(val) - 1;
        if (val == 0) {
            return 4;
        }

        int cnt = 0;
        while (val > 0) {
            cnt++;
            val >>= 1;
        }
        return cnt * 2 + 2;
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
