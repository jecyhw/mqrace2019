package io.openmessaging;

/**
 * Created by yanghuiwei on 2019-08-02
 */
public class VariableUtils {
    private static final int MAX_NUM = 2;

    public static int putSigned(byte[] data, int bitPos, int v) {
        if (v < 0) {
            put1(data, bitPos);
            return putUnsigned(data, bitPos + 1, -v);
        } else {
            return putUnsigned(data, bitPos + 1, v);
        }
    }

    public static int getSigned(byte[] data, MemoryRead memoryRead) {
        int bitPos = memoryRead.aBitPos;
        int v = 0;
        int count = 0;
        //获取符号位，0表示正数，1表是负数
        int signed = getBit(data, bitPos);

        bitPos++;

        while (true) {
            int hasData = getBit(data, bitPos);
            v |= (getBit(data, bitPos + 1) << count);
            bitPos += 2;
            if (hasData == 0) {
                memoryRead.aBitPos = bitPos;
                return signed == 0 ? v : -v;
            }
            count++;
        }
    }

    public static int putUnsigned(byte[] data, int bitPos, int v) {
        int t;
        while (true) {
            t = (v & 1);
            if (v < MAX_NUM) {
                if (t == 1) {
                    put1(data, bitPos + 1);
                }
                break;
            }
            put1(data, bitPos);
            if (t == 1) {
                put1(data, bitPos + 1);
            }
            v >>= 1;
            bitPos += 2;
        }
        return bitPos + 2;
    }

    public static int getUnsigned(byte[] data, MemoryRead memoryRead) {
        int bitPos = memoryRead.tBitPos;
        int v = 0;
        int count = 0;
        while (true) {
            int hasData = getBit(data, bitPos);
            v |= (getBit(data, bitPos + 1) << count);
            bitPos += 2;
            if (hasData == 0) {
                memoryRead.tBitPos = bitPos;
                return v;
            }
            count++;
        }
    }

    private static int getBit(byte[] data, int bitPos) {
        int pos = bitPos >> 3;
        int bit = bitPos & 7;
        return (data[pos] >>> (7 - bit)) & 1;
    }


    private static void put1(byte[] data, int bitPos) {
        int pos = bitPos >> 3;
        int bit = bitPos & 7;
        data[pos] |= (1 << (7 - bit));
    }
}
