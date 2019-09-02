package io.openmessaging.partition;

import io.openmessaging.Const;
import io.openmessaging.model.GetAvgItem;
import io.openmessaging.util.ByteBufferUtil;
import io.openmessaging.util.Utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public final class PartitionFile {
    private FileChannel aFcPool;
    private final ByteBuffer aBuf;
    private final int interval;

    public PartitionFile(int interval, String fileSuffix) {
        aBuf = ByteBuffer.allocate(interval * Const.LONG_BYTES);
        this.interval = interval;
        try {
            aFcPool = new RandomAccessFile(Const.STORE_PATH + "test" + fileSuffix, "rw").getChannel();
        } catch (FileNotFoundException e) {
            Utils.print(e.getMessage());
            System.exit(-1);
        }
    }

    public void writeA(long a) {
        if (!aBuf.hasRemaining()) {
            ByteBufferUtil.flush(aBuf, aFcPool);
        }
        aBuf.putLong(a);
    }

    public void flush() {
        ByteBufferUtil.flush(aBuf, aFcPool);
    }


    /**
     * 读一个chunk中的a的数据，注意只能是一个chunk中连续的数据，不能跨chunk
     * @param offsetCount 从第几个a开始读取
     * @param readCount 需要读取的个数
     * @param buf 缓冲区
     */
    public void readPartition(int partition, int offsetCount, int readCount, ByteBuffer buf, GetAvgItem getItem) {
        long startTime = System.currentTimeMillis();
        long filePos = ((long) (partition  * interval + offsetCount)) * Const.LONG_BYTES;

        buf.position(0);
        buf.limit(readCount * Const.LONG_BYTES);
        ByteBufferUtil.readInBuf(filePos, buf, aFcPool);
        buf.position(0);

        getItem.readChunkASortFileCount++;
        getItem.readASortFileTime += (System.currentTimeMillis() - startTime);
        getItem.readChunkASortCount += readCount;
    }


    public void log(StringBuilder sb) {
        long aSize = 0, aSortSize = 0, msgSize = 0;
        for (int i = 0; i < Const.FILE_NUMS; i++) {
            try {
                aSize += aFcPool.size();
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
