package io.openmessaging;

import java.util.concurrent.atomic.AtomicInteger;


/**
 * Created by yanghuiwei on 2019-08-02
 */
public class Memory {
    private static AtomicInteger counter = new AtomicInteger(0);
    byte[] data = new byte[Const.MEMORY_BUFFER_SIZE];

    //已使用的比特位数
    int putBitPos = 0;
    int readBitPos;

    public Memory() {
        counter.getAndIncrement();
    }

    public boolean put(int val) {
        if (putBitPos + 32 > (Const.MEMORY_BUFFER_SIZE * 8)) {
            return false;
        }

        putBitPos += VariableUtils.put(data, putBitPos, val);
        return true;
    }
}
