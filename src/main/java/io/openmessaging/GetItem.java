package io.openmessaging;

import io.openmessaging.codec.TDecoder;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by yanghuiwei on 2019-07-28
 */
public class GetItem {
    public ByteBuffer readBuf;
    public final IntervalSum intervalSum = new IntervalSum();
    public final long[] as = new long[Const.MAX_GET_AT_SIZE];
    public final long[] ts = new long[Const.MAX_GET_AT_SIZE];
    public List<Message> messages;
    public TDecoder tDecoder = new TDecoder();
    public long costTime = 0;
}
