package io.openmessaging.manager;

import io.openmessaging.Const;
import io.openmessaging.model.GetAvgItem;
import io.openmessaging.util.ByteBufferUtil;
import io.openmessaging.util.Utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public final class FileManager {
    private static FileChannel[] aFcPool = new FileChannel[Const.FILE_NUMS];
    private static FileChannel[] aSortFcPool = new FileChannel[Const.FILE_NUMS];

    private static final ByteBuffer aBuf = ByteBuffer.allocate(Const.MERGE_T_INDEX_INTERVAL * Const.LONG_BYTES);
    private static final ByteBuffer aSortBuf = ByteBuffer.allocate(Const.MERGE_T_INDEX_INTERVAL * Const.LONG_BYTES);

    private static int aFileIndex = 0;
    private static int aSortFileIndex = 0;

    public static void init(){
        try {
            for (int i = 0; i < Const.FILE_NUMS; i++) {
                aFcPool[i] = new RandomAccessFile(Const.STORE_PATH + i + Const.M_A_FILE_SUFFIX, "rw").getChannel();
                aSortFcPool[i] = new RandomAccessFile(Const.STORE_PATH + i + Const.M_A_SORT_FILE_SUFFIX, "rw").getChannel();
            }
        } catch (FileNotFoundException e) {
            Utils.print(e.getMessage());
            System.exit(-1);
        }

        SecondFileManager.init();
    }


    public static void writeA(long a) {
        if (!aBuf.hasRemaining()) {
            ByteBufferUtil.flush(aBuf, aFcPool[aFileIndex++]);
            if (aFileIndex == Const.FILE_NUMS) {
                aFileIndex = 0;
            }
        }
        aBuf.putLong(a);
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
        ByteBufferUtil.flush(aBuf, aFcPool[aFileIndex]);
        ByteBufferUtil.flush(aSortBuf, aSortFcPool[aSortFileIndex]);

        SecondFileManager.flushEnd();
    }



    /**
     * 读一个chunk中的a的数据，注意只能是一个chunk中连续的数据，不能跨chunk
     * @param beginCount 从第几个a开始读取
     * @param readCount 需要读取的个数
     * @param buf 缓冲区
     */
    public static void readChunkA(int beginCount, int readCount, ByteBuffer buf, GetAvgItem getItem) {
        long startTime = System.currentTimeMillis();
        int chunkNum = beginCount / Const.MERGE_T_INDEX_INTERVAL;
        int fileIndex = chunkNum % Const.FILE_NUMS;
        int filePos = ((chunkNum / Const.FILE_NUMS) * Const.MERGE_T_INDEX_INTERVAL + beginCount % Const.MERGE_T_INDEX_INTERVAL) * Const.LONG_BYTES;

        readChunkAOrASort(readCount, buf, aFcPool[fileIndex], filePos);
        getItem.readAFileTime += (System.currentTimeMillis() - startTime);
    }

    public static void readChunkASort(int beginCount, int readCount, ByteBuffer buf, GetAvgItem getItem) {
        long startTime = System.currentTimeMillis();
        int chunkNum = beginCount / Const.MERGE_T_INDEX_INTERVAL;
        int fileIndex = chunkNum % Const.FILE_NUMS;
        int filePos = ((chunkNum / Const.FILE_NUMS) * Const.MERGE_T_INDEX_INTERVAL + beginCount % Const.MERGE_T_INDEX_INTERVAL) * Const.LONG_BYTES;

        readChunkAOrASort(readCount, buf, aSortFcPool[fileIndex], filePos);
        getItem.readASortFileTime += (System.currentTimeMillis() - startTime);
    }


    private static void readChunkAOrASort(int readCount, ByteBuffer buf, FileChannel fc, int filePos) {
        buf.position(0);
        buf.limit(readCount * Const.LONG_BYTES);
        ByteBufferUtil.readInBuf(filePos, buf, fc);
        buf.position(0);
    }



    public static void log(StringBuilder sb) {
        long aSize = 0, aSortSize = 0, msgSize = 0;
        for (int i = 0; i < Const.FILE_NUMS; i++) {
            try {
                aSize += aFcPool[i].size();
                aSortSize += aSortFcPool[i].size();
            } catch (IOException e) {
                Utils.print("FileManager log error " + e.getMessage());
                System.exit(-1);
            }
        }
        sb.append("fileNum:").append(Const.FILE_NUMS).append(",aSize:")
                .append(aSize).append(",aSortSize:").append(aSortSize).append(",msgSize:").append(msgSize);
        sb.append("\n");

        SecondFileManager.log(sb);
    }
}
