package io.openmessaging;

import java.nio.ByteBuffer;

/**
 * Created by yanghuiwei on 2019-08-02
 */
public class Memory {
    ByteBuffer data = ByteBuffer.allocateDirect(Const.MEMORY_BUFFER_SIZE);

    //已使用的比特位数
    int putBitLength = 0;

    public boolean put(int t, int a) {
        if (hasRemaining()) {
            putBitLength = VariableUtils.putUnsigned(data, putBitLength, t);
            putBitLength = VariableUtils.putSigned(data, putBitLength, a);
            return true;
        }
        return false;
    }

    public boolean hasRemaining() {
        return putBitLength + 96 < (Const.MEMORY_BUFFER_SIZE * 8);
    }
}
