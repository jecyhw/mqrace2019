package io.openmessaging;

import java.nio.ByteBuffer;

/**
 * Created by yanghuiwei on 2019-08-23
 */
public class Encoder {
    private final ByteBuffer buf;
    private int bitsAvailable = Integer.SIZE;
    private int value = 0;
    private int delta = 0;
    private int[] stat = new int[20];

    public Encoder(ByteBuffer buf) {
        this.buf = buf;
    }

    public void encode(int newDelta) {
        int deltaOfDelta = newDelta - delta;
        if (deltaOfDelta + 8 >= 0 && deltaOfDelta + 8 < 20) {
            stat[deltaOfDelta + 8]++;
        }

        delta = newDelta;

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

        deltaOfDelta -= 4;
        boolean isNegative = false;
        if (deltaOfDelta < 0) {
            deltaOfDelta = -deltaOfDelta;
            isNegative = true;
        }

        if (deltaOfDelta < 0b10) {
            put(deltaOfDelta, 2, 0b11110, 5, isNegative);
        } else if (deltaOfDelta < 0b100) {
            put(deltaOfDelta, 3, 0b111110, 6, isNegative);
        } else if (deltaOfDelta < 0b1000) {
            put(deltaOfDelta, 4, 0b1111110, 7, isNegative);
        } else if (deltaOfDelta < 0b10000) {
            put(deltaOfDelta, 5, 0b11111110, 8, isNegative);
        } else if (deltaOfDelta < 0b100000) {
            put(deltaOfDelta, 6, 0b111111110, 9, isNegative);
        } else if (deltaOfDelta < 0b1000000) {
            put(deltaOfDelta, 7, 0b1111111110, 10, isNegative);
        } else if (deltaOfDelta < 0b10000000) {
            put(deltaOfDelta, 8, 0b11111111110, 11, isNegative);
        } else if (deltaOfDelta < 0b100000000) {
            put(deltaOfDelta, 9, 0b111111111110, 12, isNegative);
        } else if (deltaOfDelta < 0b1000000000) {
            put(deltaOfDelta, 10, 0b1111111111110, 13, isNegative);
        } else if (deltaOfDelta < 0b10000000000) {
            put(deltaOfDelta, 11, 0b11111111111110, 14, isNegative);
        } else if (deltaOfDelta < 0b100000000000) {
            put(deltaOfDelta, 12, 0b111111111111110, 15, isNegative);
        } else if (deltaOfDelta < 0b1000000000000) {
            put(deltaOfDelta, 13, 0b1111111111111110, 16, isNegative);
        } else if (deltaOfDelta < 0b10000000000000) {
            put(deltaOfDelta, 14, 0b11111111111111110, 17, isNegative);
        } else if (deltaOfDelta < 0b100000000000000) {
            put(deltaOfDelta, 15, 0b111111111111111110, 18, isNegative);
        } else if (deltaOfDelta < 0b1000000000000000) {
            put(deltaOfDelta, 16, 0b1111111111111111110, 19, isNegative);
        } else if (deltaOfDelta < 0b10000000000000000) {
            put(deltaOfDelta, 17, 0b11111111111111111110, 20, isNegative);
        } else if (deltaOfDelta < 0b100000000000000000) {
            put(deltaOfDelta, 18, 0b111111111111111111110, 21, isNegative);
        } else if (deltaOfDelta < 0b1000000000000000000) {
            put(deltaOfDelta, 19, 0b1111111111111111111110, 22, isNegative);
        } else if (deltaOfDelta < 0b10000000000000000000) {
            put(deltaOfDelta, 20, 0b11111111111111111111110, 23, isNegative);
        } else if (deltaOfDelta < 0b100000000000000000000) {
            put(deltaOfDelta, 21, 0b111111111111111111111110, 24, isNegative);
        } else if (deltaOfDelta < 0b1000000000000000000000) {
            put(deltaOfDelta, 22, 0b1111111111111111111111110, 25, isNegative);
        } else if (deltaOfDelta < 0b10000000000000000000000) {
            put(deltaOfDelta, 23, 0b11111111111111111111111110, 26, isNegative);
        } else if (deltaOfDelta < 0b100000000000000000000000) {
            put(deltaOfDelta, 24, 0b111111111111111111111111110, 27, isNegative);
        } else if (deltaOfDelta < 0b1000000000000000000000000) {
            put(deltaOfDelta, 25, 0b1111111111111111111111111110, 28, isNegative);
        } else if (deltaOfDelta < 0b10000000000000000000000000) {
            put(deltaOfDelta, 26, 0b11111111111111111111111111110, 29, isNegative);
        } else if (deltaOfDelta < 0b100000000000000000000000000) {
            put(deltaOfDelta, 27, 0b111111111111111111111111111110, 30, isNegative);
        } else if (deltaOfDelta < 0b1000000000000000000000000000) {
            put(deltaOfDelta, 28, 0b1111111111111111111111111111110, 31, isNegative);
        } else if (deltaOfDelta < 0b10000000000000000000000000000) {
            put(deltaOfDelta, 29, 0b11111111111111111111111111111110, 32, isNegative);
        }
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
            return shift;
        } else {
            value |= (bits << (-shift));
            bitsAvailable -= bitsInValue;
            return 0;
        }
    }

    private void putInt() {
        buf.putInt(value);
        bitsAvailable = Integer.SIZE;
        value = 0;
    }

    public int getBitPosition() {
        return buf.position() * 8 + bitsAvailable;
    }

    public void flush() {
        if (bitsAvailable != Integer.SIZE) {
            buf.putInt(value);
        }

        buf.flip();

        bitsAvailable = 0;
        value = 0;

        StringBuilder sb = new StringBuilder();
        sb.append("buf size:").append(buf.limit());
        for (int i = 0; i < stat.length; i++) {
            sb.append("[").append(i).append(",").append(stat[i]).append("]");
        }
        Utils.print(sb.toString());
    }

    public void resetDelta() {
        delta = 0;
    }
}
