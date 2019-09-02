package io.openmessaging.codec;

import io.openmessaging.Const;

import java.nio.ByteBuffer;

/**
 * Created by yanghuiwei on 2019-08-24
 */
public class TDecoder extends AbstractDecoder {

    public int getFirstGreatOrEqual(ByteBuffer buf, long t, long destT, int pos, int bitPos) {
        reset(buf, bitPos);
        int delta = 0;
        //从一个区间里找
        for (int i = 1; i < Const.MAX_T_INDEX_INTERVAL; i++) {
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
        for (int i = 1; i < Const.MAX_T_INDEX_INTERVAL; i++) {
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

    private int getDeltaOfDelta() {
        if (getBits(1) == 0) {
            return 0;
        } else if (getBits(1) == 0) {
            return -1;
        } else if (getBits(1) == 0) {
            return 1;
        } else if (getBits(1) == 0) {
            return -2;
        } else if (getBits(1) == 0) {
            return 2;
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
        } else if (getBits(1) == 0) {
            return getBits(3);
        } else if (getBits(1) == 0) {
            return getBits(4);
        } else if (getBits(1) == 0) {
            return getBits(5);
        } else if (getBits(1) == 0) {
            return getBits(6);
        } else if (getBits(1) == 0) {
            return getBits(7);
        } else if (getBits(1) == 0) {
            return getBits(8);
        } else if (getBits(1) == 0) {
            return getBits(9);
        } else if (getBits(1) == 0) {
            return getBits(10);
        } else if (getBits(1) == 0) {
            return getBits(11);
        } else if (getBits(1) == 0) {
            return getBits(12);
        } else if (getBits(1) == 0) {
            return getBits(13);
        } else if (getBits(1) == 0) {
            return getBits(14);
        } else if (getBits(1) == 0) {
            return getBits(15);
        } else if (getBits(1) == 0) {
            return getBits(16);
        } else if (getBits(1) == 0) {
            return getBits(17);
        } else if (getBits(1) == 0) {
            return getBits(18);
        } else if (getBits(1) == 0) {
            return getBits(19);
        } else if (getBits(1) == 0) {
            return getBits(20);
        } else if (getBits(1) == 0) {
            return getBits(21);
        } else if (getBits(1) == 0) {
            return getBits(22);
        } else if (getBits(1) == 0) {
            return getBits(23);
        } else if (getBits(1) == 0) {
            return getBits(24);
        } else if (getBits(1) == 0) {
            return getBits(25);
        } else if (getBits(1) == 0) {
            return getBits(26);
        } else if (getBits(1) == 0) {
            return getBits(27);
        } else if (getBits(1) == 0) {
            return getBits(28);
        } else if (getBits(1) == 0) {
            return getBits(29);
        }
        return 0;
    }
}
