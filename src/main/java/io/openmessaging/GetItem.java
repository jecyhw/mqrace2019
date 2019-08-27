package io.openmessaging;

import io.openmessaging.codec.ADecoder;
import io.openmessaging.codec.MsgDecoder;
import io.openmessaging.codec.TDecoder;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by yanghuiwei on 2019-07-28
 */
public class GetItem {
    final ByteBuffer buf = ByteBuffer.allocate(Const.GET_BUFFER_SIZE);
    final IntervalSum intervalSum = new IntervalSum();
    final long[] as = new long[Const.MAX_GET_AT_SIZE];
    final long[] ts = new long[Const.MAX_GET_AT_SIZE];
    List<Message> messages;
    ByteBuffer[] tBufs;
    TDecoder tDecoder = new TDecoder();
    ADecoder aDecoder = new ADecoder();
    MsgDecoder msgDecoder = new MsgDecoder();
}
