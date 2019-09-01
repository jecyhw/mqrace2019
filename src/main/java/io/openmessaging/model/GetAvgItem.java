package io.openmessaging.model;

import io.openmessaging.Const;
import io.openmessaging.codec.TDecoder;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by yanghuiwei on 2019-07-28
 */
public class GetAvgItem {
    public ByteBuffer readBuf = ByteBuffer.allocate(Const.MERGE_T_INDEX_INTERVAL * Const.MSG_BYTES);
    public final IntervalSum intervalSum = new IntervalSum();
    public final long[] as = new long[Const.MAX_GET_AT_SIZE];
    public TDecoder tDecoder = new TDecoder();

    public ExecutorService executorService = Executors.newFixedThreadPool(2, r -> {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        return thread;
    });
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
}
