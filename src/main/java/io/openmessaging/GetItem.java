package io.openmessaging;

import java.nio.ByteBuffer;

/**
 * Created by yanghuiwei on 2019-07-28
 */
public class GetItem {
    ByteBuffer buf = ByteBuffer.allocateDirect(Const.GET_BUFFER_SIZE);
    IntervalSum intervalSum = new IntervalSum();
    int[] as = new int[Const.MAX_GET_MSG_SIZE];
    int[] ts = new int[Const.MAX_GET_MSG_SIZE];
    int[] sortPos = new int[Const.PUT_THREAD_SIZE];
}
