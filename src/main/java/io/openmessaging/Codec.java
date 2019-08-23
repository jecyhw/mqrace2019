package io.openmessaging;

import java.nio.ByteBuffer;

/**
 * Created by yanghuiwei on 2019-08-23
 */
public class Codec {
    private static final int ZERO = 0;
    private static final int ONE = 0b10;
    private static final int TWO = 0b110;
    private static final int THREE = 0b1110;
    private static final int BITS_AVAILABLE = Integer.SIZE;

    private static final int[] NUM_BIT_LENGTH = new int[] {
        1, 2, 3, 4, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6, 6, 6, 6
    };

    private static final int[] NUM_BIT_FLAG = new int[] {
            0b0, 0b10,
            0b110, 0b1110,
            0b11110, 0b11110, 0b11110, 0b11110,
            0b111110, 0b111110, 0b111110, 0b111110, 0b111110, 0b111110, 0b111110, 0b111110
    };


    private ByteBuffer buf;
    private int bitsAvailable = Integer.SIZE;
    private int value = 0;
    private int bitBos1 = 0;
    private int bitBos2 = 0;
    private int delta = 0;


    public void encode(int newDelta) {
        encode1(newDelta);
        if (newDelta == ZERO) {
            bitBos1++;
            return;
        }

        if (newDelta == ONE) {
            bitBos1 += 2;
            return;
        }

        if (newDelta == TWO) {
            bitBos1 += 3;
            return;
        }
        if (newDelta == THREE) {
            bitBos1 += 4;
            return;
        }


        bitBos1 += 2;
        int t = newDelta, cnt = 0;
        while (t > 0) {
            cnt++;
            t >>= 1;
        }
        bitBos1 += (cnt << 1);

    }

    public void encode1(int newDelta) {
        int deltaOfDelta = newDelta - delta;
        if (deltaOfDelta == ZERO) {
            bitBos2++;
            return;
        }

        if (deltaOfDelta == ONE) {
            bitBos2 += 2;
            return;
        }

        if (deltaOfDelta == TWO) {
            bitBos2 += 3;
            return;
        }
        if (deltaOfDelta == THREE) {
            bitBos2 += 4;
            return;
        }


        bitBos2 += 3;
        int t = Math.abs(deltaOfDelta), cnt = 0;
        while (t > 0) {
            cnt++;
            t >>= 1;
        }
        bitBos2 += (cnt << 1);

        delta = Math.abs(newDelta);
    }

    public void flush() {
        Utils.print("bitBos1:" + bitBos1 / 8 + " bitBos2:" + bitBos2 / 8);
    }

    public int getUnsigned(ByteBuffer buf, int bitOffset, int[] dest, int pos) {
        int v = 0;
        int count = 0;
        int aByte = buf.get(bitOffset >> 3);

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

        while (true) {
            hasData = (aByte >>> (bitOffset & 7)) & 1;

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

    private void put1(ByteBuffer buf, int bitPos) {
        int pos = bitPos >> 3;
        int bit = bitPos & 7;
        buf.put(pos, (byte) (buf.get(pos) | (1 << bit)));
    }
}
