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
import java.util.ArrayList;
import java.util.List;

public final class SinglePartitionFile {
    private final List<FileChannel> aFcPool = new ArrayList<>();
    private FileChannel aFc;
    private final ByteBuffer aBuf;
    private int aFileIndex = 0;
    private int putACount = 0;

    private final int interval;
    private final String fileSuffix;


    public SinglePartitionFile(int interval, String fileSuffix) {
        this.fileSuffix = fileSuffix;
        aBuf = ByteBuffer.allocate(interval * Const.LONG_BYTES);
        this.interval = interval;
        newAFc();
    }

    private void newAFc() {
        try {
            aFc = new RandomAccessFile(Const.STORE_PATH + aFileIndex + Const.M_A_FILE_SUFFIX, "rw").getChannel();
            aFcPool.add(aFc);
        } catch (FileNotFoundException e) {
            Utils.print(e.getMessage());
            System.exit(-1);
        }
    }

    public void writeA(long a) {
        if (!aBuf.hasRemaining()) {
            ByteBufferUtil.flush(aBuf, aFc);
        }
        aBuf.putLong(a);


        //如果putAAndMsgCount等于FILE_STORE_MSG_NUM，也要刷盘，因为要切文件
        if (++putACount == Const.FILE_STORE_MSG_NUM) {
            ByteBufferUtil.flush(aBuf, aFc);

            aFileIndex++;
            newAFc();
            putACount = 0;
        }
    }

    public void flush() {
        ByteBufferUtil.flush(aBuf, aFcPool.get(aFileIndex));

    }

    public void readPartition(int offsetCount, int readCount, ByteBuffer buf, GetAvgItem getItem) {
        long startTime = System.currentTimeMillis();

        int fileIndex = offsetCount / Const.FILE_STORE_MSG_NUM;
        int fileOffsetCount = offsetCount % Const.FILE_STORE_MSG_NUM;

        if (fileOffsetCount + readCount <= Const.FILE_STORE_MSG_NUM) {
            _readPartition(readCount, buf, aFcPool.get(fileIndex), fileOffsetCount * ((long) Const.LONG_BYTES));
        } else {
            int prevReadCount = Const.FILE_STORE_MSG_NUM - fileOffsetCount;
            _readPartition(prevReadCount, buf, aFcPool.get(fileIndex), fileOffsetCount * ((long)Const.LONG_BYTES));

            buf.limit(readCount * Const.LONG_BYTES);
            buf.position(prevReadCount * Const.LONG_BYTES);
            ByteBufferUtil.readInBuf(0, buf, aFcPool.get(fileIndex + 1));
            buf.position(0);
        }

        getItem.readFileACount++;
        getItem.readFileATime += (System.currentTimeMillis() - startTime);
        getItem.readACount += readCount;
    }

    private void _readPartition(int readCount, ByteBuffer buf, FileChannel fc, long filePos) {
        buf.position(0);
        buf.limit(readCount * Const.LONG_BYTES);
        ByteBufferUtil.readInBuf(filePos, buf, fc);
        buf.position(0);
    }

    public void log(StringBuilder sb) {
        long aSize = 0, aSortSize = 0, msgSize = 0;
        for (int i = 0; i < aFileIndex + 1; i++) {
            try {
                aSize += aFcPool.get(i).size();
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
