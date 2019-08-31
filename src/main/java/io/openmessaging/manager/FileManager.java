package io.openmessaging.manager;

import io.openmessaging.Const;
import io.openmessaging.GetItem;
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
    }


    public static void writeA(long a) {
        if (!aBuf.hasRemaining()) {
            flush(aBuf, aFcPool[aFileIndex++]);
            if (aFileIndex == Const.FILE_NUMS) {
                aFileIndex = 0;
            }
        }
        aBuf.putLong(a);
    }

    public static void writeASort(long a) {
        if (!aSortBuf.hasRemaining()) {
            flush(aSortBuf, aSortFcPool[aSortFileIndex++]);
            if (aSortFileIndex == Const.FILE_NUMS) {
                aSortFileIndex = 0;
            }
        }
        aSortBuf.putLong(a);
    }

    public static void flushEnd() {
        flush(aBuf, aFcPool[aFileIndex]);
        flush(aSortBuf, aSortFcPool[aSortFileIndex]);
    }

    private static void flush(ByteBuffer byteBuffer, FileChannel fc){
        byteBuffer.flip();
        try {
            while (byteBuffer.hasRemaining()){
                fc.write(byteBuffer);
            }
        } catch (IOException e) {
            Utils.print("func=flush " + e.getMessage());
            System.exit(-1);
        }
        byteBuffer.clear();
    }

    /**
     * 读一个chunk中的a的数据，注意只能是一个chunk中连续的数据，不能跨chunk
     * @param beginCount 从第几个a开始读取
     * @param as 用来接收读取的结果
     * @param readCount 需要读取的个数
     * @param buf 缓冲区
     */
    public static void readChunkA(int beginCount, long[] as, int readCount, ByteBuffer buf, GetItem getItem) {
        long startTime = System.currentTimeMillis();
        int chunkNum = beginCount / Const.MERGE_T_INDEX_INTERVAL;
        int fileIndex = chunkNum % Const.FILE_NUMS;
        int filePos = ((chunkNum / Const.FILE_NUMS) * Const.MERGE_T_INDEX_INTERVAL + beginCount % Const.MERGE_T_INDEX_INTERVAL) * Const.LONG_BYTES;

        readChunkAOrASort(as, readCount, buf, aFcPool[fileIndex], filePos);
        getItem.readAFileTime += (System.currentTimeMillis() - startTime);
    }

    public static void readChunkASort(int beginCount, long[] as, int readCount, ByteBuffer buf, GetItem getItem) {
        long startTime = System.currentTimeMillis();
        int chunkNum = beginCount / Const.MERGE_T_INDEX_INTERVAL;
        int fileIndex = chunkNum % Const.FILE_NUMS;
        int filePos = ((chunkNum / Const.FILE_NUMS) * Const.MERGE_T_INDEX_INTERVAL + beginCount % Const.MERGE_T_INDEX_INTERVAL) * Const.LONG_BYTES;

        readChunkAOrASort(as, readCount, buf, aSortFcPool[fileIndex], filePos);
        getItem.readASortFileTime += (System.currentTimeMillis() - startTime);
    }


    private static void readChunkAOrASort(long[] as, int readCount, ByteBuffer buf, FileChannel fc, int filePos) {

        //TODO 目前简单实现，一次读完
        buf.position(0);
        buf.limit(readCount * Const.LONG_BYTES);

        readInBuf(filePos, buf, fc);

        buf.position(0);
        for (int i = 0; i < readCount; i++) {
            as[i] = buf.getLong();
        }
    }


    private static void readInBuf(long pos, ByteBuffer bb, FileChannel fc) {
        try {
            while (bb.hasRemaining()) {
                int read = fc.read(bb, pos);
                pos += read;
            }
        } catch (IOException e) {
            Utils.print(e.getMessage() + " method:readInBuf");
            System.exit(-1);
        }
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
    }
}
