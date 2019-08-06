package io.openmessaging;

import java.nio.ByteBuffer;

/**
 * Created by yanghuiwei on 2019-08-02
 */
public class Memory {
    ByteBuffer data = ByteBuffer.allocateDirect(Const.MEMORY_BUFFER_SIZE);

    //已使用的比特位数
    int putBitLength = 0;

    public boolean putUnsigned(int val) {
        if (hasRemaining()) {
            putBitLength = VariableUtils.putUnsigned(data, putBitLength, val);
            return true;
        }
        return false;
    }


    public boolean putSigned(int val) {
        if (hasRemaining()) {
            putBitLength = VariableUtils.putSigned(data, putBitLength, val);
            return true;
        }
        return false;
    }


    public boolean hasRemaining() {
        return putBitLength + 64 < (Const.MEMORY_BUFFER_SIZE * 8);
    }
}
