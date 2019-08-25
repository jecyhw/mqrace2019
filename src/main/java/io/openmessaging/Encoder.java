package io.openmessaging;

import java.nio.ByteBuffer;

/**
 * Created by yanghuiwei on 2019-08-23
 */
public class Encoder extends AbstractEncoder {

    private int delta = 0;

    public Encoder(ByteBuffer buf) {
        super(buf);
    }

    public void encode(int newDelta) {
        int deltaOfDelta = newDelta - delta;
        delta = newDelta;

        if (deltaOfDelta == 0) {
            putOnce(0, 1);
            return;
        }
        if (deltaOfDelta == -1) {
            put(0b10, 2);
            return;
        }
        if (deltaOfDelta == 1) {
            put(0b110, 3);
            return;
        }
        if (deltaOfDelta == -2) {
            put(0b1110, 4);
            return;
        } else if (deltaOfDelta == 2) {
            put(0b11110, 5);
            return;
        }

        if (deltaOfDelta < 0) {
            deltaOfDelta = ((-deltaOfDelta - Const.T_DECREASE) << 1) | 1;

        } else {
            deltaOfDelta = (deltaOfDelta - Const.T_DECREASE) << 1;
        }

        if (deltaOfDelta < 0b10) {
            put(deltaOfDelta, 2, 0b111110, 6);
        } else if (deltaOfDelta < 0b100) {
            put(deltaOfDelta, 3, 0b1111110, 7);
        } else if (deltaOfDelta < 0b1000) {
            put(deltaOfDelta, 4, 0b11111110, 8);
        } else if (deltaOfDelta < 0b10000) {
            put(deltaOfDelta, 5, 0b111111110, 9);
        } else if (deltaOfDelta < 0b100000) {
            put(deltaOfDelta, 6, 0b1111111110, 10);
        } else if (deltaOfDelta < 0b1000000) {
            put(deltaOfDelta, 7, 0b11111111110, 11);
        } else if (deltaOfDelta < 0b10000000) {
            put(deltaOfDelta, 8, 0b111111111110, 12);
        } else if (deltaOfDelta < 0b100000000) {
            put(deltaOfDelta, 9, 0b1111111111110, 13);
        } else if (deltaOfDelta < 0b1000000000) {
            put(deltaOfDelta, 10, 0b11111111111110, 14);
        } else if (deltaOfDelta < 0b10000000000) {
            put(deltaOfDelta, 11, 0b111111111111110, 15);
        } else if (deltaOfDelta < 0b100000000000) {
            put(deltaOfDelta, 12, 0b1111111111111110, 16);
        } else if (deltaOfDelta < 0b1000000000000) {
            put(deltaOfDelta, 13, 0b11111111111111110, 17);
        } else if (deltaOfDelta < 0b10000000000000) {
            put(deltaOfDelta, 14, 0b111111111111111110, 18);
        } else if (deltaOfDelta < 0b100000000000000) {
            put(deltaOfDelta, 15, 0b1111111111111111110, 19);
        } else if (deltaOfDelta < 0b1000000000000000) {
            put(deltaOfDelta, 16, 0b11111111111111111110, 20);
        } else if (deltaOfDelta < 0b10000000000000000) {
            put(deltaOfDelta, 17, 0b111111111111111111110, 21);
        } else if (deltaOfDelta < 0b100000000000000000) {
            put(deltaOfDelta, 18, 0b1111111111111111111110, 22);
        } else if (deltaOfDelta < 0b1000000000000000000) {
            put(deltaOfDelta, 19, 0b11111111111111111111110, 23);
        } else if (deltaOfDelta < 0b10000000000000000000) {
            put(deltaOfDelta, 20, 0b111111111111111111111110, 24);
        } else if (deltaOfDelta < 0b100000000000000000000) {
            put(deltaOfDelta, 21, 0b1111111111111111111111110, 25);
        } else if (deltaOfDelta < 0b1000000000000000000000) {
            put(deltaOfDelta, 22, 0b11111111111111111111111110, 26);
        } else if (deltaOfDelta < 0b10000000000000000000000) {
            put(deltaOfDelta, 23, 0b111111111111111111111111110, 27);
        } else if (deltaOfDelta < 0b100000000000000000000000) {
            put(deltaOfDelta, 24, 0b1111111111111111111111111110, 28);
        } else if (deltaOfDelta < 0b1000000000000000000000000) {
            put(deltaOfDelta, 25, 0b11111111111111111111111111110, 29);
        } else if (deltaOfDelta < 0b10000000000000000000000000) {
            put(deltaOfDelta, 26, 0b111111111111111111111111111110, 30);
        } else if (deltaOfDelta < 0b100000000000000000000000000) {
            put(deltaOfDelta, 27, 0b1111111111111111111111111111110, 31);
        } else if (deltaOfDelta < 0b1000000000000000000000000000) {
            put(deltaOfDelta, 28, 0b11111111111111111111111111111110, 32);
        } else {
            System.out.println();
        }
    }

    private void put(int bits, int bitsInValue, int controlValue, int controlValueBitLength) {
        put(controlValue, controlValueBitLength);
        put(bits, bitsInValue);
    }


    public void resetDelta() {
        delta = 0;
    }
}
