package io.openmessaging;

import java.nio.ByteBuffer;

/**
 * Created by yanghuiwei on 2019-07-28
 */
public class GetItem {
    ByteBuffer buf = ByteBuffer.allocateDirect(Const.GET_BUFFER_SIZE);
    IntervalSum intervalSum = new IntervalSum();
    int[] as = new int[80 * 1000000 + 100];
    int[] ts = new int[80 * 10000 + 100];
}
