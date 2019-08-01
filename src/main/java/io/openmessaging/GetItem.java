package io.openmessaging;

import java.nio.ByteBuffer;

/**
 * Created by yanghuiwei on 2019-07-28
 */
public class GetItem {
    ByteBuffer buf = ByteBuffer.allocateDirect(Const.GET_BUFFER_SIZE);
    ByteBuffer keyBuf = ByteBuffer.allocate(Const.LONG_BYTES);
    int maxCount = 0;
    int maxActualCount = 0;
}
