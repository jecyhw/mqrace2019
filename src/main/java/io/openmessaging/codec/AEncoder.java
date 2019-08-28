package io.openmessaging.codec;

import io.openmessaging.Const;

import java.nio.ByteBuffer;

/**
 * Created by yanghuiwei on 2019-08-25
 */
public class AEncoder extends AbstractEncoder {

    public AEncoder(ByteBuffer buf) {
        super(buf);
    }

    private int lastABitsAvailable = 0;

    public void encodeFirst(long a) {
        lastABitsAvailable = 0;
        encode(a);
    }


    public void encode(long a) {
        int aBitsAvailable = getNumBitsAvailable(a);
        int deltaOfABitsAvailable = aBitsAvailable - lastABitsAvailable;

        encodeLength(deltaOfABitsAvailable);
        putLong(a, aBitsAvailable);

        lastABitsAvailable = aBitsAvailable;
    }

    private void encodeLength(int deltaOfABitsAvailable) {
        if (deltaOfABitsAvailable == 0) {
            putOnce(0, 1);
            return;
        }
        if (deltaOfABitsAvailable == -1) {
            put(0b10, 2);
            return;
        }
        if (deltaOfABitsAvailable == 1) {
            put(0b110, 3);
            return;
        }
        if (deltaOfABitsAvailable == -2) {
            put(0b1110, 4);
            return;
        } else if (deltaOfABitsAvailable == 2) {
            put(0b11110, 5);
            return;
        } else if (deltaOfABitsAvailable == -3) {
            put(0b111110, 6);
            return;
        } else if (deltaOfABitsAvailable == 3) {
            put(0b1111110, 7);
            return;
        } else {
            int sign = 0;
            if (deltaOfABitsAvailable < 0) {
                deltaOfABitsAvailable = -deltaOfABitsAvailable;
                sign = 1;
            }
            deltaOfABitsAvailable = deltaOfABitsAvailable - 4;
            int deltaOfABitsAvailableSign = (deltaOfABitsAvailable << 1) | sign;

            if (deltaOfABitsAvailable < 0b10) {
                put(deltaOfABitsAvailableSign, 2, 0b11111110, 8);
            } else if (deltaOfABitsAvailable < 0b100) {
                put(deltaOfABitsAvailableSign, 3, 0b111111110, 9);
            } else if (deltaOfABitsAvailable < 0b1000) {
                put(deltaOfABitsAvailableSign, 4, 0b1111111110, 10);
            } else if (deltaOfABitsAvailable < 0b10000) {
                put(deltaOfABitsAvailableSign, 5, 0b11111111110, 11);
            } else if (deltaOfABitsAvailable < 0b100000) {
                put(deltaOfABitsAvailableSign, 6, 0b111111111110, 12);
            } else if (deltaOfABitsAvailable < 0b1000000) {
                put(deltaOfABitsAvailableSign, 7, 0b1111111111110, 13);
            } else if (deltaOfABitsAvailable < 0b10000000) {
                put(deltaOfABitsAvailableSign, 8, 0b11111111111110, 14);
            } else if (deltaOfABitsAvailable < 0b100000000) {
                put(deltaOfABitsAvailableSign, 9, 0b111111111111110, 15);
            }
        }
    }

    private void put(int bits, int bitsInValue, int controlValue, int controlValueBitLength) {
        put(controlValue, controlValueBitLength);
        put(bits, bitsInValue);
    }

    public boolean hasRemaining() {
        return buf.remaining() > Const.INDEX_INTERVAL * Const.LONG_BYTES;
    }

    public void resetBuf(ByteBuffer buf) {
        this.buf = buf;
        this.reset();
    }

    public long getLongBitPosition() {
        return buf.position() * 8L + (Integer.SIZE - bitsAvailable);
    }

}
