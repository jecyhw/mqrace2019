package io.openmessaging.codec;

import io.openmessaging.Const;
import io.openmessaging.util.ByteUtil;

import java.nio.ByteBuffer;

/**
 * Created by yanghuiwei on 2019-08-26
 */
public class MsgEncoder extends AbstractEncoder {
    private long v1, v2, v3, v4;
    private int v5;

    public MsgEncoder(ByteBuffer buf) {
        super(buf);
    }

    public void encodeFirst(byte[] body) {
        v1 = ByteUtil.getLong(body, 0);
        v2 = ByteUtil.getLong(body, 8);
        v3 = ByteUtil.getLong(body, 16);
        v4 = ByteUtil.getLong(body, 24);
        v5 = ByteUtil.getShort(body, 32);
        putLongVal(v1);
        putLongVal(v2);
        putLongVal(v3);
        putLongVal(v4);
        putShortVal(v5);
    }

    public void encode(byte[] body) {
        long nv1 = ByteUtil.getLong(body, 0);
        long nv2 = ByteUtil.getLong(body, 8);
        long nv3 = ByteUtil.getLong(body, 16);
        long nv4 = ByteUtil.getLong(body, 24);
        int nv5 = ByteUtil.getShort(body, 32);

        putLongVal(nv1 - v1);
        putLongVal(nv2 - v2);
        putLongVal(nv3 - v3);
        putLongVal(nv4 - v4);
        putShortVal(nv5 - v5);

        v1 = nv1;
        v2 = nv2;
        v3 = nv3;
        v4 = nv4;
        v5 = nv5;
    }

    private void putLongVal(long val) {
        int signed = 0;
        if (val < 0) {
            signed = 1;
            val = -val;
        }
        int bitsAvailable = getNumBitsAvailable(val);
        put(((bitsAvailable - 1) << 1) | signed, 7);
        putLong(val, bitsAvailable);
    }

    private void putShortVal(int val) {
        int signed = 0;
        if (val < 0) {
            signed = -1;
            val = -val;
        }
        int bitsAvailable = getNumBitsAvailable(val);
        put(((bitsAvailable - 1) << 1) | signed, 5);
        put(val, bitsAvailable);
    }

    public boolean hasRemaining() {
        return buf.remaining() - Const.MSG_BYTES * 4 > 0;
    }

}
