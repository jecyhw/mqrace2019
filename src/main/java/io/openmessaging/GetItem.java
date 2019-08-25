package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yanghuiwei on 2019-07-28
 */
public class GetItem {
    final ByteBuffer buf = ByteBuffer.allocate(Const.GET_BUFFER_SIZE);
    final IntervalSum intervalSum = new IntervalSum();
    final long[] as = new long[Const.MAX_GET_MSG_SIZE];
    final long[] ts = new long[Const.MAX_GET_MSG_SIZE];
    final byte[] uncompressMsgData = new byte[Const.COMPRESS_MSG_SIZE];
    List<ByteBuffer> tBufs = new ArrayList<>();
    Decoder decoder = new Decoder();
}
