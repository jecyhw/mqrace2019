package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yanghuiwei on 2019-07-28
 */
public class GetItem {
    ByteBuffer buf = ByteBuffer.allocateDirect(Const.GET_BUFFER_SIZE);
    int maxCount = 0;
    int maxActualCount = 0;
    MemoryRead memoryRead = new MemoryRead();
    MemoryGetItem minItem = new MemoryGetItem();
    MemoryGetItem maxItem = new MemoryGetItem();

    int[] as = new int[40 * 10000];

    List<Message> messages = new ArrayList<>();
    int messageSize = 0;
}
