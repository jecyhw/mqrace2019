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

    public int getFirstGreatOrEqual(ByteBuffer buf, long t, long destT, int pos, int bitPos) {
        reset(buf, bitPos);
        int delta = 0;
        //从一个区间里找
        for (int i = 0; i < Const.INDEX_INTERVAL - 1; i++) {
            delta += getDeltaOfDelta();
            t = t + delta;
            if (t >= destT) {
                return pos;
            }
            pos++;
        }
        //没找到下一个区间的第一个值肯定符合
        return pos;
    }

    public int getFirstGreat(ByteBuffer buf, long t, long destT, int pos, int bitPos) {
        reset(buf, bitPos);
        int delta = 0;
        //从一个区间里找
        for (int i = 0; i < Const.INDEX_INTERVAL - 1; i++) {
            delta += getDeltaOfDelta();
            t = t + delta;
            if (t > destT) {
                return pos;
            }
            pos++;
        }
        //没找到继续从下一个区间找
        return -1;
    }

    public void decode(ByteBuffer buf, long[] t, int tPos, int bitPos, int readLen) {
        reset(buf, bitPos);
        int delta = 0;
        for (int i = 1; i <= readLen; i++, tPos++) {
            delta += getDeltaOfDelta();
            t[tPos] = t[tPos - 1] + delta;
        }
    }

    private void reset(ByteBuffer buf, int bitPos) {
        this.buf = buf;
        //不能改成除以8，否则会出错
        buf.position((bitPos / 32) * 4);
        bits = buf.getInt();
        bitsAvailable = Integer.SIZE - bitPos % 32;
    }

    private int getDeltaOfDelta() {
        if (getBits(1) == 0) {
            return 0;
        } else if (getBits(1) == 0) {
            return  -1;
        } else if (getBits(1) == 0) {
            return  1;
        } else if (getBits(1) == 0) {
            return  -2;
        } else if (getBits(1) == 0) {
            return  2;
        } else {
            return getAdaptiveDeltaOfDelta(getAdaptive());
        }
    }

    private int getAdaptiveDeltaOfDelta(int bitsValue) {
        return (bitsValue & 1) == 0 ? ((bitsValue >> 1) + Const.T_DECREASE) : -((bitsValue >> 1) + Const.T_DECREASE);
    }

    private int getAdaptive() {
        if (getBits(1) == 0) {
            return getBits(2);
        }
        else  if (getBits(1) == 0) {
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
        else   if (getBits(1) == 0) {
            return getBits(8);
        }
        else  if (getBits(1) == 0) {
            return getBits(9);
        }
        else  if (getBits(1) == 0) {
            return getBits(10);
        }
        else  if (getBits(1) == 0) {
            return getBits(11);
        }
        else   if (getBits(1) == 0) {
            return getBits(12);
        }
        else  if (getBits(1) == 0) {
            return getBits(13);
        }
        else  if (getBits(1) == 0) {
            return getBits(14);
        }
        else if (getBits(1) == 0) {
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
        if (getBits(1) == 0) {
            return getBits(27);
        }
        if (getBits(1) == 0) {
            return getBits(28);
        }
        if (getBits(1) == 0) {
            return getBits(29);
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
        bits = buf.getInt();
        bitsAvailable = Integer.SIZE;
    }
}
