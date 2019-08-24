package io.openmessaging;

import java.nio.ByteBuffer;

/**
 * Created by yanghuiwei on 2019-08-24
 */
public class Decoder {
    private final ByteBuffer buf;

    private int bitsAvailable = Integer.SIZE;
    private int value = 0;

    private int delta = 0;


    public Decoder(ByteBuffer buf) {
        this.buf = buf;
    }

    public int decode(int baseT, int bitPos, int delta) {

        buf.position(bitPos / 8);
        value = buf.getInt();
        bitsAvailable = bitPos % 8;

        if (get(1) == 0) {
            return 0;
        }
        if (get(1) == 0) {
            return 1;
        }
        if (get(1) == 0) {
            return 2;
        }
        if (get(1) == 0) {
            return 3;
        }
        if (get(1) == 0) {
            return get(3);
        }
        if (get(1) == 0) {
            return get(4);
        }
        if (get(1) == 0) {
            return get(5);
        }
        if (get(1) == 0) {
            return get(6);
        }
        if (get(1) == 0) {
            return get(7);
        }
        if (get(1) == 0) {
            return get(8);
        }
        if (get(1) == 0) {
            return get(9);
        }
        if (get(1) == 0) {
            return get(10);
        }
        if (get(1) == 0) {
            return get(11);
        }
        if (get(1) == 0) {
            return get(12);
        }
        if (get(1) == 0) {
            return get(13);
        }
        if (get(1) == 0) {
            return get(14);
        }
        if (get(1) == 0) {
            return get(15);
        }
        if (get(1) == 0) {
            return get(16);
        }
        if (get(1) == 0) {
            return get(17);
        }
        if (get(1) == 0) {
            return get(18);
        }
        if (get(1) == 0) {
            return get(19);
        }
        if (get(1) == 0) {
            return get(20);
        }
        if (get(1) == 0) {
            return get(21);
        }
        if (get(1) == 0) {
            return get(22);
        }
        if (get(1) == 0) {
            return get(23);
        }
        if (get(1) == 0) {
            return get(24);
        }
        if (get(1) == 0) {
            return get(25);
        }
        if (get(1) == 0) {
            return get(26);
        }
        if (get(1) == 0) {
            return get(27);
        }
        if (get(1) == 0) {
            return get(28);
        }
        if (get(1) == 0) {
            return get(29);
        }
        if (get(1) == 0) {
            return get(30);
        }
        //会执行出错
        return 0;
    }


    public int get(int bitsInValue) {
        int value = 0;
        while (bitsInValue > 0) {
            if (bitsInValue >= bitsAvailable) {
                byte lsb = (byte) (value & ((1 << bitsAvailable) - 1));
                value = (value << bitsAvailable) + (lsb & 0xff);
                bitsInValue -= bitsAvailable;
                bitsAvailable = 0;
            } else {
                byte lsb = (byte) ((value >>> (bitsAvailable - bitsInValue)) & ((1 << bitsInValue) - 1));
                value = (value << bitsInValue) + (lsb & 0xff);
                bitsAvailable -= bitsInValue;
                bitsInValue = 0;
            }
            readFromByteBuffer();
        }
        return value;
    }

    private void readFromByteBuffer() {
        if (bitsAvailable == 0) {
            value = buf.getInt();
            bitsAvailable = Integer.SIZE;
        }
    }

}
