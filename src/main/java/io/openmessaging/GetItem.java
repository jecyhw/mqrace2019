package io.openmessaging;

import java.nio.ByteBuffer;

/**
 * Created by yanghuiwei on 2019-07-28
 */
public class GetItem {
    ByteBuffer buf = ByteBuffer.allocateDirect(Const.GET_BUFFER_SIZE);
    IntervalSum intervalSum = new IntervalSum();
    long[] as = new long[Const.MAX_GET_MSG_SIZE];
    long[] ts = new long[Const.MAX_GET_MSG_SIZE];
}
