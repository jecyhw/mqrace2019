package io.openmessaging;

import java.nio.ByteBuffer;

/**
 * Created by yanghuiwei on 2019-08-24
 */
public class Decoder {
    private ByteBuffer buf;

    private int bitsAvailable = Integer.SIZE;
    private int bitsInValue;
    private int bits = 0;

    public void decode(ByteBuffer buf, long[] t, int tPos, int bitPos, int readLen) {
        this.buf = buf;
        buf.position(bitPos / 8);
        bits = buf.getInt();
        bitsAvailable = Integer.SIZE - bitPos % 8;

        int delta = 0;
        for (int i = 1; i <= readLen; i++, tPos++) {
            int deltaOfDelta = 0;
            if (getBits(1) != 0) {
                if (getBits(1) == 0) {
                    deltaOfDelta = 1;
                } else if (getBits(1) == 0) {
                    deltaOfDelta = 2;
                } else if (getBits(1) == 0) {
                    deltaOfDelta = 3;
                } else {
                    int bitsValue = getAdaptive();
                    deltaOfDelta = getDeltaOfDelta(bitsValue);
                }
            }
            delta += deltaOfDelta;
            t[tPos] = t[tPos - 1] + delta;
        }
    }

    private int getDeltaOfDelta(int bitsValue) {
        return (bitsValue & 1) == 0 ? bitsValue >> 1 : -(bitsValue >> 1);
    }

    private int _decode(long[] t, int pos, int delta) {

        return delta;
    }

    private int getAdaptive() {
        if (getBits(1) == 0) {
            return getBits(3);
        }
        else  if (getBits(1) == 0) {
            return getBits(4);
        }
        else  if (getBits(1) == 0) {
            return getBits(5);
        }
        else  if (getBits(1) == 0) {
            return getBits(6);
        }
        else  if (getBits(1) == 0) {
            return getBits(7);
        }
        else  if (getBits(1) == 0) {
            return getBits(8);
        }
        else   if (getBits(1) == 0) {
            return getBits(9);
        }
        else  if (getBits(1) == 0) {
            return getBits(10);
        }
        else  if (getBits(1) == 0) {
            return getBits(11);
        }
        else  if (getBits(1) == 0) {
            return getBits(12);
        }
        else   if (getBits(1) == 0) {
            return getBits(13);
        }
        else  if (getBits(1) == 0) {
            return getBits(14);
        }
        else  if (getBits(1) == 0) {
            return getBits(15);
        }
        else if (getBits(1) == 0) {
            return getBits(16);
        }
        else if (getBits(1) == 0) {
            return getBits(17);
        }
        else if (getBits(1) == 0) {
            return getBits(18);
        }
        else if (getBits(1) == 0) {
            return getBits(19);
        }
        else if (getBits(1) == 0) {
            return getBits(20);
        }
        else if (getBits(1) == 0) {
            return getBits(21);
        }
        else if (getBits(1) == 0) {
            return getBits(22);
        }
        else if (getBits(1) == 0) {
            return getBits(23);
        }
        else if (getBits(1) == 0) {
            return getBits(24);
        }
        else if (getBits(1) == 0) {
            return getBits(25);
        }
        else if (getBits(1) == 0) {
            return getBits(26);
        }
        else if (getBits(1) == 0) {
            return getBits(27);
        }
        if (getBits(1) == 0) {
            return getBits(28);
        }
        if (getBits(1) == 0) {
            return getBits(29);
        }
        if (getBits(1) == 0) {
            return getBits(30);
        }
        return 0;
    }


    public int getBits(int bitsInValue) {
        this.bitsInValue = bitsInValue;
        int res = getOnce(0);
        if (this.bitsInValue > 0) {
            res = getOnce(res);
        }
        return res;
    }

    private int getOnce(int res) {
        if (bitsInValue >= bitsAvailable) {
            int lsb = (bits & ((1 << bitsAvailable) - 1));
            res = (res << bitsAvailable) + lsb;
            bitsInValue -= bitsAvailable;
            getInt();
        } else {
            int lsb =  ((bits >>> (bitsAvailable - bitsInValue)) & ((1 << bitsInValue) - 1));
            res = (res << bitsInValue) + lsb;
            bitsAvailable -= bitsInValue;
            bitsInValue = 0;
        }
        return res;
    }

    private void getInt() {
        if (bitsAvailable == 0) {
            bits = buf.getInt();
            bitsAvailable = Integer.SIZE;
        }
    }

}
