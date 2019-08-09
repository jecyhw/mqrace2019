package io.openmessaging;

import java.nio.ByteBuffer;

/**
 * Created by yanghuiwei on 2019-08-02
 */
public class Memory {
    UnsafeMemory data = new UnsafeMemory(Const.MEMORY_BUFFER_SIZE);
    //已使用的比特位数
    int putBitLength = 0;

    public void put(int t, int a) {
        putBitLength = VariableUtils.putUnsigned(data, putBitLength, t);
        putBitLength = VariableUtils.putSigned(data, putBitLength, a);
    }
}
