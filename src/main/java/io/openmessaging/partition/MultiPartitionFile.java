package io.openmessaging.partition;

import io.openmessaging.Const;
import io.openmessaging.model.GetAvgItem;
import io.openmessaging.util.ByteBufferUtil;
import io.openmessaging.util.Utils;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public final class MultiPartitionFile {
    private final FileChannel[] aFcPool = new FileChannel[Const.FILE_NUMS];
    private final ByteBuffer aBuf;
    private int aFileIndex = 0;
    private final int interval;

    public MultiPartitionFile(int interval, String fileSuffix) {
        aBuf = ByteBuffer.allocateDirect(interval * Const.LONG_BYTES);
        this.interval = interval;
        try {
            for (int i = 0; i < Const.FILE_NUMS; i++) {
                aFcPool[i] = new RandomAccessFile(Const.STORE_PATH + i + fileSuffix, "rw").getChannel();
            }
        } catch (FileNotFoundException e) {
            Utils.print(e.getMessage());
            System.exit(-1);
        }
    }

    public void writeA(long a) {
        if (!aBuf.hasRemaining()) {
            ByteBufferUtil.flush(aBuf, aFcPool[aFileIndex++]);
            if (aFileIndex == Const.FILE_NUMS) {
                aFileIndex = 0;
            }
        }
        aBuf.putLong(a);
    }

    public void flush() {
        ByteBufferUtil.flush(aBuf, aFcPool[aFileIndex]);
    }


    /**
     * 读一个chunk中的a的数据，注意只能是一个chunk中连续的数据，不能跨chunk
     * @param offsetCount 从第几个a开始读取
     * @param readCount 需要读取的个数
     * @param buf 缓冲区
     */
    public void readPartition(int partition, int offsetCount, int readCount, ByteBuffer buf, GetAvgItem getItem) {
//        long startTime = System.currentTimeMillis();
        int fileIndex = partition % Const.FILE_NUMS;
        long filePos = ((long)(partition / Const.FILE_NUMS) * interval + offsetCount) * Const.LONG_BYTES;

        buf.position(0);
        buf.limit(readCount * Const.LONG_BYTES);
        ByteBufferUtil.readInBuf(filePos, buf, aFcPool[fileIndex]);
        buf.position(0);

//        getItem.readASortFileCount++;
//        getItem.readASortFileTime += (System.currentTimeMillis() - startTime);
//        getItem.readASortCount += readCount;
    }
}
