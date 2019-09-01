package io.openmessaging.manager;

import io.openmessaging.Const;
import io.openmessaging.model.GetAvgItem;
import io.openmessaging.util.ByteBufferUtil;
import io.openmessaging.util.Utils;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by yanghuiwei on 2019-09-01
 */
public class SecondFileManager {
    private static FileChannel[] aSortFcPool = new FileChannel[Const.FILE_NUMS];

    private static final ByteBuffer aSortBuf = ByteBuffer.allocate(Const.MERGE_T_INDEX_INTERVAL * Const.LONG_BYTES);

    private static int aSortFileIndex = 0;

    public static void init(){
        try {
            for (int i = 0; i < Const.FILE_NUMS; i++) {
                aSortFcPool[i] = new RandomAccessFile(Const.STORE_PATH + i + Const.M_A_SORT_FILE_SUFFIX, "rw").getChannel();
            }
        } catch (FileNotFoundException e) {
            Utils.print(e.getMessage());
            System.exit(-1);
        }
    }


    public static void writeASort(long a) {
        if (!aSortBuf.hasRemaining()) {
            ByteBufferUtil.flush(aSortBuf, aSortFcPool[aSortFileIndex++]);
            if (aSortFileIndex == Const.FILE_NUMS) {
                aSortFileIndex = 0;
            }
        }
        aSortBuf.putLong(a);
    }

    public static void flushEnd() {
        ByteBufferUtil.flush(aSortBuf, aSortFcPool[aSortFileIndex]);
    }


    public static void readChunkASort(int beginCount, long[] as, int readCount, ByteBuffer buf, GetAvgItem getItem) {
        long startTime = System.currentTimeMillis();
        int chunkNum = beginCount / Const.MERGE_T_INDEX_INTERVAL;
        int fileIndex = chunkNum % Const.FILE_NUMS;
        int filePos = ((chunkNum / Const.FILE_NUMS) * Const.MERGE_T_INDEX_INTERVAL + beginCount % Const.MERGE_T_INDEX_INTERVAL) * Const.LONG_BYTES;


        buf.position(0);
        buf.limit(readCount * Const.LONG_BYTES);

        ByteBufferUtil.readInBuf(filePos, buf, aSortFcPool[fileIndex]);

        buf.position(0);
        for (int i = 0; i < readCount; i++) {
            as[i] = buf.getLong();
        }
        getItem.readASortFileTime += (System.currentTimeMillis() - startTime);
    }
}
