package io.openmessaging;

import java.nio.ByteBuffer;

/**
 * Created by yanghuiwei on 2019-08-23
 */
public class Codec {
    private static final int ZERO = 0;
    private static final int ONE = 0b10;
    private static final int TWO = 0b110;
    private static final int POSITIVE_FLAG = 0b111;
    private static final int NEGIVATE_FLAG = 0b1111;

    private ByteBuffer buf;
    private int bitBos;
    private int bitsAvailable = Integer.SIZE;
    private int delta = 0;
    private int minDeltaOfDelta = Integer.MAX_VALUE;
    private int maxDeltaOfDelta = Integer.MIN_VALUE;

    public void encode(int newDelta) {
        int deltaOfDelta = newDelta - delta;

        minDeltaOfDelta = Math.min(minDeltaOfDelta, deltaOfDelta);
        maxDeltaOfDelta = Math.max(maxDeltaOfDelta, deltaOfDelta);

        if (deltaOfDelta == ZERO) {
            bitBos++;
            return;
        }
        if (deltaOfDelta == ONE) {
            bitBos += 2;
            return;
        }
        if (deltaOfDelta == TWO) {
            bitBos += 3;
            return;
        }
        if (deltaOfDelta > 0) {
            bitBos += 32 + 3;
        } else {
            bitBos += 32 + 4;
        }

        delta = Math.abs(newDelta);
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
