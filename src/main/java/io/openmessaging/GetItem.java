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

    public ByteBuffer aIndexArr;
    public ByteBuffer aSumArr;
    public long costTime = 0;
    public int readChunkAFileCount = 0;
    public int readChunkASortFileCount = 0;
    public int sumChunkASortFileCount = 0;
    public int readChunkACount = 0;
    public int readChunkASortCount = 0;
    public int sumChunkASortCount = 0;
    public int readFirstOrLastASortCount = 0;
    public int readCount = 0;
    public int readHitCount = 0;
    public long readAFileTime = 0;
    public long readASortFileTime = 0;
}
