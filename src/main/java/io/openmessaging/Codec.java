package io.openmessaging;

import java.nio.ByteBuffer;

/**
 * Created by yanghuiwei on 2019-08-23
 */
public class Codec {
    private static final int ZERO = 0;
    private static final int BITS_AVAILABLE = Integer.SIZE;
    private static final int[] NUM_BIT_LEN = new int[]{0, 0, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3};

    private ByteBuffer buf;
    private int bitBos;
    private int bitsAvailable = Integer.SIZE;
    private int delta = 0;

    public void encode(int newDelta) {
        if (newDelta == ZERO) {
            bitBos++;
            return;
        }
        bitBos++;
        if (newDelta < 16) {
            bitBos += (NUM_BIT_LEN[newDelta] << 1);
        } else {
            int t = newDelta, cnt = 0;
            while (t > 0) {
                cnt++;
                t >>= 1;
            }
            bitBos += (cnt << 1);
        }

        delta = newDelta;
    }

    public void flush() {
        Utils.print("bitBos:" + bitBos / 8);
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
