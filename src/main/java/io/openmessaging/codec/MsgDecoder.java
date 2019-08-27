package io.openmessaging.codec;

import io.openmessaging.util.ByteUtil;

/**
 * Created by yanghuiwei on 2019-08-26
 */
public class MsgDecoder extends AbstractDecoder {
    private long v1, v2, v3, v4;
    private int v5;

    public void decodeFirst(byte[] body) {
        v1 = getLongVal();
        v2 = getLongVal();
        v3 = getLongVal();
        v4 = getLongVal();
        v5 = getShortVal();

        ByteUtil.putLong(body, v1, 0);
        ByteUtil.putLong(body, v2, 8);
        ByteUtil.putLong(body, v3, 16);
        ByteUtil.putLong(body, v4, 24);
        ByteUtil.putShort(body, v5, 32);

    }

    public void decodeFirstDiscard() {
        v1 = getLongVal();
        v2 = getLongVal();
        v3 = getLongVal();
        v4 = getLongVal();
        v5 = getShortVal();
    }

    public void decode(byte[] body) {
        long dv1 = v1, dv2 = v2, dv3 = v3, dv4 = v4;
        int dv5 = v5;
        dv1 += getLongVal();
        dv2 += getLongVal();
        dv3 += getLongVal();
        dv4 += getLongVal();
        dv5 += getShortVal();

        ByteUtil.putLong(body, dv1, 0);
        ByteUtil.putLong(body, dv2, 8);
        ByteUtil.putLong(body, dv3, 16);
        ByteUtil.putLong(body, dv4, 24);
        ByteUtil.putShort(body, dv5, 32);

        v1 = dv1;
        v2 = dv2;
        v3 = dv3;
        v4 = dv4;
        v5 = dv5;
    }

    public void decodeDiscard() {
        v1 += getLongVal();
        v2 += getLongVal();
        v3 += getLongVal();
        v4 += getLongVal();
        v5 += getShortVal();
    }

    private long getLongVal() {
        int bitsAvailable = getBits(7);
        int signed = bitsAvailable & 1;
        bitsAvailable = bitsAvailable >> 1;
        long val = getLong(bitsAvailable + 1);
        return signed == 0 ? val : -val;
    }

    private int getShortVal() {
        int bitsAvailable = getBits(5);
        int signed = bitsAvailable & 1;
        bitsAvailable = bitsAvailable >> 1;
        int val = getBits(bitsAvailable + 1);
        return signed == 0 ? val : -val;
    }

}
