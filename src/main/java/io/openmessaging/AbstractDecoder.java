package io.openmessaging;

import java.nio.ByteBuffer;

/**
 * Created by yanghuiwei on 2019-08-25
 */
public abstract class AbstractDecoder {
    ByteBuffer buf;

    int bitsAvailable = Integer.SIZE;
    int bitsInValue;
    int bits = 0;

    /**
     * 每次读取前需要重置下buf的开始位置
     * @param buf
     * @param bitPos
     */
    public void reset(ByteBuffer buf, int bitPos) {
        //记录buf，准备读取
        this.buf = buf;
        //不能改成除以8，否则会出错
        buf.position((bitPos / 32) * 4);
        bits = buf.getInt();
        bitsAvailable = Integer.SIZE - bitPos % 32;
    }

    /**
     * 从buf中拿多少比特，bitsInValue=4表示从buf中往后拿4个比特位
     * @param bitsInValue 最大31位
     * @return
     */
    public int getBits(int bitsInValue) {
        this.bitsInValue = bitsInValue;
        int res = getOnce(0);
        if (this.bitsInValue > 0) {
            res = getOnce(res);
        }
        return res;
    }

    private int getOnce(int res) {
        if (bitsInValue >= bitsAvailable) {
            int lsb = (bits & ((1 << bitsAvailable) - 1));
            res = (res << bitsAvailable) + lsb;
            bitsInValue -= bitsAvailable;
            getInt();
        } else {
            int lsb =  ((bits >>> (bitsAvailable - bitsInValue)) & ((1 << bitsInValue) - 1));
            res = (res << bitsInValue) + lsb;
            bitsAvailable -= bitsInValue;
            bitsInValue = 0;
        }
        return res;
    }

    private void getInt() {
        bits = buf.getInt();
        bitsAvailable = Integer.SIZE;
    }
}
