package io.openmessaging;

/**
 * Created by yanghuiwei on 2019-08-02
 */
public class Memory {
    byte[] data = new byte[Const.MEMORY_BUFFER_SIZE];

    //已使用的比特位数
    int putBitLength = 0;

    public boolean put(int val) {
        if (hasRemaining()) {
            putBitLength = VariableUtils.put(data, putBitLength, val);
            return true;
        }
        return false;
    }

    public boolean hasRemaining() {
        return putBitLength + 32 < (Const.MEMORY_BUFFER_SIZE * 8);
    }
}
