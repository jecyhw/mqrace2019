package io.openmessaging;

import java.nio.ByteBuffer;

/**
 * Created by yanghuiwei on 2019-08-25
 */
public abstract class AbstractEncoder {
    ByteBuffer buf;
    int bitsAvailable = Integer.SIZE;
    int value = 0;

    public AbstractEncoder() {

    }

    public AbstractEncoder(ByteBuffer buf) {
        this.buf = buf;
    }

    /**
     * 把bits数放到buf中 example: bits=18 bitsInValue=5
     * @param bits
     * @param bitsInValue 数的有效二进制位的个数，最大31位
     */
    public void put(int bits, int bitsInValue) {
        bitsInValue = putOnce(bits, bitsInValue);
        if (bitsInValue > 0) {
            putOnce(bits, bitsInValue);
        }
    }

    int putOnce(int bits, int bitsInValue) {
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

    /**
     * 获取当前的比特位个数
     * @return
     */
    public int getBitPosition() {
        return buf.position() * 8 + (Integer.SIZE - bitsAvailable);
    }

    /**
     * 将最后一个数据刷到buf中
     */
    public void flush() {
        if (bitsAvailable != Integer.SIZE) {
            buf.putInt(value);
        }
    }

    /**
     * 不在put时，将value写入到buf中
     */
    public void flushAndClear() {
        flush();
        reset();
    }

    private void reset() {
        bitsAvailable = 0;
        value = 0;
        buf.clear();
    }

    int getNumBitsAvailable(long num) {
        int t = (int) (num >> 32);
        if (t == 0) {
            return 32 - _getNumBitsAvailable((int) num);
        } else {
            return 64 - _getNumBitsAvailable(t);
        }
    }

    private int _getNumBitsAvailable(int a) {
        int n;
        if (a == 0) {
            return 31;
        }
        n = 1;
        if ((a >>> 16) == 0) {
            n = n + 16;
            a = a << 16;
        }
        if ((a >>> 24) == 0) {
            n = n + 8;
            a = a << 8;
        }
        if ((a >>> 28) == 0) {
            n = n + 4;
            a = a << 4;
        }
        if ((a >>> 30) == 0) {
            n = n + 2;
            a = a << 2;
        }
        n = n - (a >>> 31);
        return n;
    }
}
