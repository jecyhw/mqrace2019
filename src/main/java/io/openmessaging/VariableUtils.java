package io.openmessaging;

import java.nio.ByteBuffer;

/**
 * Created by yanghuiwei on 2019-08-02
 */
public class VariableUtils {
    private static final int MAX_NUM = 2;

    public static int putSigned(ByteBuffer buf, int bitPos, int v) {
        v = v - Const.A_DECREASE;
        if (v < 0) {
            put1(buf, bitPos);
            return putUnsigned(buf, bitPos + 1, -v);
        } else {
            return putUnsigned(buf, bitPos + 1, v);
        }
    }

    public static int getSigned(ByteBuffer buf, int bitOffset, int[] dest, int pos) {
        int v = 0;
        int count = 0;
        //获取符号位，0表示正数，1表是负数
        int signed = getBit(buf, bitOffset);

        bitOffset++;

        while (true) {
            int hasData = getBit(buf, bitOffset);
            v |= (getBit(buf, bitOffset + 1) << count);
            bitOffset += 2;
            if (hasData == 0) {
                dest[pos] = (signed == 0 ? v : -v) + Const.A_DECREASE;
                return bitOffset;
            }
            count++;
        }
    }

    public static int putUnsigned(ByteBuffer buf, int bitPos, int v) {
        int t;
        while (true) {
            t = (v & 1);
            if (v < MAX_NUM) {
                if (t == 1) {
                    put1(buf, bitPos + 1);
                }
                break;
            }
            put1(buf, bitPos);
            if (t == 1) {
                put1(buf, bitPos + 1);
            }
            v >>= 1;
            bitPos += 2;
        }
        return bitPos + 2;
    }

    public static int getUnsigned(ByteBuffer buf, int bitOffset, int[] dest, int pos) {
        int v = 0;
        int count = 0;
        while (true) {
            int hasData = getBit(buf, bitOffset);
            v |= (getBit(buf, bitOffset + 1) << count);
            bitOffset += 2;
            if (hasData == 0) {
                dest[pos] = v;
                return bitOffset;
            }
            count++;
        }
    }

    private static int getBit(ByteBuffer buf, int bitPos) {
        int pos = bitPos >> 3;
        return getByte(buf.get(pos), bitPos);
    }

    private static int getByte(byte b, int bitPos) {
        return (b >>> (bitPos & 7)) & 1;
    }

    private static void put1(ByteBuffer buf, int bitPos) {
        int pos = bitPos >> 3;
        int bit = bitPos & 7;
        buf.put(pos, (byte) (buf.get(pos) | (1 << bit)));
    }
}
