package io.openmessaging;

import java.nio.ByteBuffer;

/**
 * Created by yanghuiwei on 2019-08-23
 */
public class Encoder {
    private ByteBuffer buf = ByteBuffer.allocateDirect(Const.MEMORY_BUFFER_SIZE);
    private int bitsAvailable = Integer.SIZE;
    private int value = 0;

    private int delta = 0;
    private int minDeltaOfDelta = Integer.MAX_VALUE;
    private int maxDeltaOfDelta = Integer.MIN_VALUE;


    public void encode(int newDelta) {
        int deltaOfDelta = newDelta - delta;

        minDeltaOfDelta = Math.min(minDeltaOfDelta, deltaOfDelta);
        maxDeltaOfDelta = Math.max(maxDeltaOfDelta, deltaOfDelta);

        if (deltaOfDelta == 0) {
            putOnce(0, 1);
            return;
        }
        if (deltaOfDelta == 1) {
            put(0b10, 2);
            return;
        }
        if (deltaOfDelta == 2) {
            put(0b110, 3);
            return;
        }
        if (deltaOfDelta == 3) {
            put(0b1110, 4);
            return;
        }

        boolean isNegative = false;
        if (deltaOfDelta < 0) {
            deltaOfDelta = -deltaOfDelta;
            isNegative = true;
        }

        if (deltaOfDelta < 0b1000) {
            put(deltaOfDelta, 3, 0b11110, 5, isNegative);
        } else if (deltaOfDelta < 0b10000) {
            put(deltaOfDelta, 4, 0b111110, 6, isNegative);
        } else if (newDelta < 0b100000) {
            put(newDelta, 5, 0b1111110, 7, isNegative);
        } else if (newDelta < 0b1000000) {
            put(newDelta, 6, 0b11111110, 8, isNegative);
        } else if (newDelta < 0b10000000) {
            put(newDelta, 7, 0b111111110, 9, isNegative);
        } else if (newDelta < 0b100000000) {
            put(newDelta, 8, 0b1111111110, 10, isNegative);
        } else if (newDelta < 0b1000000000) {
            put(newDelta, 9, 0b11111111110, 11, isNegative);
        } else if (newDelta < 0b10000000000) {
            put(newDelta, 10, 0b111111111110, 12, isNegative);
        } else if (newDelta < 0b100000000000) {
            put(newDelta, 11, 0b1111111111110, 13, isNegative);
        } else if (newDelta < 0b1000000000000) {
            put(newDelta, 12, 0b11111111111110, 14, isNegative);
        } else if (newDelta < 0b10000000000000) {
            put(newDelta, 13, 0b111111111111110, 15, isNegative);
        } else if (newDelta < 0b100000000000000) {
            put(newDelta, 14, 0b1111111111111110, 16, isNegative);
        } else if (newDelta < 0b1000000000000000) {
            put(newDelta, 15, 0b11111111111111110, 17, isNegative);
        } else if (newDelta < 0b10000000000000000) {
            put(newDelta, 16, 0b111111111111111110, 18, isNegative);
        } else if (newDelta < 0b100000000000000000) {
            put(newDelta, 17, 0b1111111111111111110, 19, isNegative);
        } else if (newDelta < 0b1000000000000000000) {
            put(newDelta, 18, 0b11111111111111111110, 20, isNegative);
        } else if (newDelta < 0b10000000000000000000) {
            put(newDelta, 19, 0b111111111111111111110, 21, isNegative);
        } else if (newDelta < 0b100000000000000000000) {
            put(newDelta, 20, 0b1111111111111111111110, 22, isNegative);
        } else if (newDelta < 0b1000000000000000000000) {
            put(newDelta, 21, 0b11111111111111111111110, 23, isNegative);
        } else if (newDelta < 0b10000000000000000000000) {
            put(newDelta, 22, 0b111111111111111111111110, 24, isNegative);
        } else if (newDelta < 0b100000000000000000000000) {
            put(newDelta, 23, 0b1111111111111111111111110, 25, isNegative);
        } else if (newDelta < 0b1000000000000000000000000) {
            put(newDelta, 24, 0b11111111111111111111111110, 26, isNegative);
        } else if (newDelta < 0b10000000000000000000000000) {
            put(newDelta, 25, 0b111111111111111111111111110, 27, isNegative);
        } else if (newDelta < 0b100000000000000000000000000) {
            put(newDelta, 26, 0b1111111111111111111111111110, 28, isNegative);
        } else if (newDelta < 0b1000000000000000000000000000) {
            put(newDelta, 27, 0b11111111111111111111111111110, 29, isNegative);
        } else if (newDelta < 0b10000000000000000000000000000) {
            put(newDelta, 28, 0b111111111111111111111111111110, 30, isNegative);
        } else if (newDelta < 0b100000000000000000000000000000) {
            put(newDelta, 29, 0b1111111111111111111111111111110, 31, isNegative);
        } else if (newDelta < 0b1000000000000000000000000000000) {
            put(newDelta, 30, 0b11111111111111111111111111111110, 32, isNegative);
        }

        delta = newDelta > 0 ? newDelta : -newDelta;
    }

    private void put(int bits, int bitsInValue, int controlValue, int controlValueBitLength, boolean isNegative) {
        put(controlValue, controlValueBitLength);

        if (isNegative) {
            bits = (bits << 1) | 1;
        }
        put(bits, bitsInValue + 1);
    }

    private void put(int bits, int bitsInValue) {
        bitsInValue = putOnce(bits, bitsInValue);
        if (bitsInValue > 0) {
            putOnce(bits, bitsInValue);
        }
    }

    private int putOnce(int bits, int bitsInValue) {
        int shift = bitsInValue - bitsAvailable;
        if (shift >= 0) {
            value |= ((bits >> shift) & (1 << bitsAvailable) - 1);
            putInt();
            return bitsInValue - bitsAvailable;
        } else {
            shift = bitsAvailable - bitsInValue;
            value |= (bits << shift);
            bitsAvailable -= bitsInValue;
            return 0;
        }
    }

    private void putInt() {
        buf.putInt(value);
        bitsAvailable = Integer.SIZE;
        value = 0;
    }

    private int getPrevDelta() {
        return delta;
    }

    private int getBitPosition() {
        return buf.position() * 8 + bitsAvailable;
    }

    public void flush() {
        if (bitsAvailable != Integer.SIZE) {
            buf.putInt(value);
        }

        buf.flip();

        bitsAvailable = 0;
        value = 0;
        Utils.print("buf size:" + buf.limit() + " min:" + minDeltaOfDelta + " max:" + maxDeltaOfDelta);
    }

}
