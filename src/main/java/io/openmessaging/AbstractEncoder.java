package io.openmessaging;

import java.nio.ByteBuffer;

/**
 * Created by yanghuiwei on 2019-08-25
 */
public abstract class AbstractEncoder {
    private final ByteBuffer buf;
    private int bitsAvailable = Integer.SIZE;
    private int value = 0;

    public AbstractEncoder(ByteBuffer buf) {
        this.buf = buf;
    }

    void put(int bits, int bitsInValue) {
        bitsInValue = putOnce(bits, bitsInValue);
        if (bitsInValue > 0) {
            putOnce(bits, bitsInValue);
        }
    }

    int putOnce(int bits, int bitsInValue) {
        int shift = bitsInValue - bitsAvailable;
        if (shift >= 0) {
            value |= ((bits >> shift) & (1 << bitsAvailable) - 1);
            putInt();
            return shift;
        } else {
            value |= (bits << (-shift));
            bitsAvailable -= bitsInValue;
            return 0;
        }
    }

    void putInt() {
        buf.putInt(value);
        bitsAvailable = Integer.SIZE;
        value = 0;
    }

    int getBitPosition() {
        return buf.position() * 8 + (Integer.SIZE - bitsAvailable);
    }

    void flush() {
        if (bitsAvailable != Integer.SIZE) {
            buf.putInt(value);
        }

        bitsAvailable = 0;
        value = 0;
        buf.clear();
    }
}
