package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public final class AMemory {
    private static AtomicInteger counter = new AtomicInteger(0);

    public static ByteBuffer getCacheBuf() {
        int index = counter.incrementAndGet();
        if (index > Const.A_MEMORY_IN_HEAP_NUM) {
            if (index > Const.A_MEMORY_IN_NUM) {
                return ByteBuffer.allocateDirect(Const.A_MEMORY_LAST_OUT_HEAP_SIZE);
            } else {
                return ByteBuffer.allocateDirect(Const.A_MEMORY_OUT_HEAP_SIZE);
            }
        } else {
            return ByteBuffer.allocate(Const.A_MEMORY_IN_HEAP_SIZE);
        }
    }
}
