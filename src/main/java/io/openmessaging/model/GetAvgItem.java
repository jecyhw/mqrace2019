package io.openmessaging.model;

import io.openmessaging.Const;
import io.openmessaging.codec.TDecoder;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by yanghuiwei on 2019-07-28
 */
public class GetAvgItem {
    public ByteBuffer readBuf = ByteBuffer.allocate(Const.MERGE_T_INDEX_INTERVAL * Const.MSG_BYTES);
    public final IntervalSum intervalSum = new IntervalSum();
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
    public int readHitCount = 0;
    public long readAFileTime = 0;
    public long readASortFileTime = 0;
    public Map<Integer, Integer> map = new HashMap<>();

}
