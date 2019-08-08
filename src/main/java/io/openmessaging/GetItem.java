package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yanghuiwei on 2019-07-28
 */
public class GetItem {
    ByteBuffer buf = ByteBuffer.allocateDirect(Const.GET_BUFFER_SIZE);
    IntervalSum intervalSum = new IntervalSum();
    int[] as = new int[80 * 10000 + 100];
    int[] ts = new int[80 * 10000 + 100];

    List<Message> cacheMessages = new ArrayList<>();

}
