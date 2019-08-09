package io.openmessaging;

/**
 * Created by yanghuiwei on 2019-08-02
 */
public class VariableUtils {
    private static final int MAX_NUM = 2;

    public static int putSigned(UnsafeMemory buf, int bitPos, int v) {
        v = v - Const.A_DECREASE;
        if (v < 0) {
            put1(buf, bitPos);
            return putUnsigned(buf, bitPos + 1, -v);
        } else {
            return putUnsigned(buf, bitPos + 1, v);
        }
    }

    public static int getSigned(UnsafeMemory buf, int bitOffset, int[] dest, int pos) {
        int v = 0;
        int count = 0;

        int aByte = buf.get(bitOffset >> 3);
        //获取符号位，0表示正数，1表是负数
        int signed =  (aByte >>> (bitOffset & 7)) & 1;

        if ((++bitOffset & 7) == 0) {
            aByte = buf.get(bitOffset >> 3);
        }

        while (true) {
            int hasData = (aByte >>> (bitOffset & 7)) & 1;

            if ((++bitOffset & 7) == 0) {
                aByte = buf.get(bitOffset >> 3);
            }

            v |= (((aByte >>> ((bitOffset++) & 7)) & 1) << count);

            if (hasData == 0) {
                dest[pos] = (signed == 0 ? v : -v) + Const.A_DECREASE;
                return bitOffset;
            }

            if ((bitOffset & 7) == 0) {
                aByte = buf.get(bitOffset >> 3);
            }
            count++;
        }
    }

    public static int putUnsigned(UnsafeMemory buf, int bitPos, int v) {
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

    public static int getUnsigned(UnsafeMemory buf, int bitOffset, int[] dest, int pos) {
        int v = 0;
        int count = 0;
        int aByte = buf.get(bitOffset >> 3);
        while (true) {
            int hasData = (aByte >>> (bitOffset & 7)) & 1;

            if ((++bitOffset & 7) == 0) {
                aByte = buf.get(bitOffset >> 3);
            }

            v |= (((aByte >>> ((bitOffset++) & 7)) & 1) << count);

            if (hasData == 0) {
                dest[pos] = v;
                return bitOffset;
            }

            if ((bitOffset & 7) == 0) {
                aByte = buf.get(bitOffset >> 3);
            }
            count++;
        }
    }

    private static void put1(UnsafeMemory buf, int bitPos) {
        int pos = bitPos >> 3;
        int bit = bitPos & 7;
        buf.put(pos, (byte) (buf.get(pos) | (1 << bit)));
    }
}
