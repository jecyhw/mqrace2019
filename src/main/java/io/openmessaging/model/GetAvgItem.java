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
    public ByteBuffer readBuf = ByteBuffer.allocate(Const.MAX_T_INDEX_INTERVAL * Const.MSG_BYTES);
    public final IntervalSum intervalSum = new IntervalSum();
    public TDecoder tDecoder = new TDecoder();

    public long costTime = 0;
    public int readASortFileCount = 0;
    public long readASortFileTime = 0;
    public int readASortCount = 0;

    public int readFileACount = 0;
    public long readFileATime = 0;
    public int readACount = 0;

    public int readHitCount = 0;
    public Map<Integer, Integer> map = new HashMap<>();
    public Map<Integer, Integer> countMap = new HashMap<>();

}
