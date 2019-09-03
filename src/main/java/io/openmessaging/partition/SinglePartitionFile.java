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
    private final ByteBuffer aCacheBuf1 = ByteBuffer.allocate(Const.A_CACHE_BYTES1);
    private final ByteBuffer aCacheBuf2 = ByteBuffer.allocate(Const.A_CACHE_BYTES2);


    public SinglePartitionFile(int interval) {
        aBuf = ByteBuffer.allocateDirect(interval * Const.LONG_BYTES);
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
        if (aCacheBuf1.hasRemaining()) {
            aCacheBuf1.putLong(a);
        } else if (aCacheBuf2.hasRemaining()) {
            aCacheBuf2.putLong(a);
        } else {

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
    }

    public void flush() {
        ByteBufferUtil.flush(aBuf, aFcPool.get(aFileIndex));

    }

    public void readPartition(int offsetCount, int readCount, ByteBuffer reavBuf, GetAvgItem getItem) {
        int endCount = offsetCount + readCount;
        if (endCount <= Const.A_CACHE_BYTES1 / Const.LONG_BYTES) {
            //全在缓存1里面
            readFromCache(aCacheBuf1, offsetCount, readCount, reavBuf, 0);
        } else if (offsetCount < Const.A_CACHE_BYTES1 / Const.LONG_BYTES){
            //缓存1和缓存2都有
            int readCountFromCache1 = Const.A_CACHE_BYTES1 / Const.LONG_BYTES - offsetCount;
            readFromCache(aCacheBuf1, offsetCount, readCountFromCache1, reavBuf, 0);
            readFromCache(aCacheBuf2, 0, readCount - readCountFromCache1, reavBuf, readCountFromCache1);
        } else if (endCount <= (Const.A_CACHE_BYTES1 / Const.LONG_BYTES + Const.A_CACHE_BYTES2 / Const.LONG_BYTES)) {
            //只在缓存2中
            readFromCache(aCacheBuf2, offsetCount - Const.A_CACHE_BYTES1 / Const.LONG_BYTES, readCount, reavBuf, 0);
        } else if (offsetCount < (Const.A_CACHE_BYTES1 / Const.LONG_BYTES + Const.A_CACHE_BYTES2 / Const.LONG_BYTES)) {
            //缓存2和文件都有
            int readCountFromCache2 = (Const.A_CACHE_BYTES1 / Const.LONG_BYTES + Const.A_CACHE_BYTES2 / Const.LONG_BYTES) - offsetCount;
            readFromCache(aCacheBuf2, offsetCount - Const.A_CACHE_BYTES1 / Const.LONG_BYTES, readCountFromCache2, reavBuf, 0);
            readFromFile(0, readCount - readCountFromCache2, reavBuf, readCountFromCache2, getItem);
        } else {
            //只在文件中
            readFromFile(offsetCount - (Const.A_CACHE_BYTES1 / Const.LONG_BYTES + Const.A_CACHE_BYTES2 / Const.LONG_BYTES),
                    readCount, reavBuf, 0, getItem);
        }
        reavBuf.position(0);
    }

    private void readFromFile(int offsetCount, int readCount, ByteBuffer recvBuf, int recvOffsetCount, GetAvgItem getItem) {
        long startTime = System.currentTimeMillis();

        int fileIndex = offsetCount / Const.FILE_STORE_MSG_NUM;
        int fileOffsetCount = offsetCount % Const.FILE_STORE_MSG_NUM;

        if (fileOffsetCount + readCount <= Const.FILE_STORE_MSG_NUM) {
            _readPartition(readCount, recvBuf, recvOffsetCount, aFcPool.get(fileIndex), fileOffsetCount * ((long) Const.LONG_BYTES));
        } else {
            int prevReadCount = Const.FILE_STORE_MSG_NUM - fileOffsetCount;
            _readPartition(prevReadCount, recvBuf, recvOffsetCount, aFcPool.get(fileIndex), fileOffsetCount * ((long)Const.LONG_BYTES));

            recvBuf.limit(readCount * Const.LONG_BYTES);
            recvBuf.position(prevReadCount * Const.LONG_BYTES);
            ByteBufferUtil.readInBuf(0, recvBuf, aFcPool.get(fileIndex + 1));
        }

        getItem.readFileACount++;
        getItem.readFileATime += (System.currentTimeMillis() - startTime);
        getItem.readACount += readCount;
    }

    public void readFromCache(ByteBuffer aCacheBuf, int startCount, int readCount, ByteBuffer recvBuf, int recvStartCount) {
        ByteBuffer duplicate = aCacheBuf.duplicate();
        duplicate.limit((startCount + readCount) * Const.LONG_BYTES);
        duplicate.position(startCount * Const.LONG_BYTES);

        recvBuf.limit((recvStartCount + readCount) * Const.LONG_BYTES);
        recvBuf.position(recvStartCount * Const.LONG_BYTES);

        recvBuf.put(duplicate);
    }

    private void _readPartition(int readCount, ByteBuffer buf, int recvOffsetCount, FileChannel fc, long filePos) {
        buf.position(recvOffsetCount * Const.LONG_BYTES);
        buf.limit((recvOffsetCount + readCount) * Const.LONG_BYTES);
        ByteBufferUtil.readInBuf(filePos, buf, fc);
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
