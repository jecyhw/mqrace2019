package io.openmessaging.manager;

import io.openmessaging.Const;
import io.openmessaging.util.Utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public final class FileManager {
    private static List<FileChannel> aFcPool = new ArrayList<>();
    private static List<FileChannel> aSortFcPool = new ArrayList<>();

    private static FileChannel aFc;
    private static FileChannel aSortFc;

    private static final ByteBuffer aBuf = ByteBuffer.allocate(Const.M_PUT_BUFFER_SIZE);
    private static final ByteBuffer aSortBuf = ByteBuffer.allocate(Const.M_PUT_BUFFER_SIZE);

    private static int putAAndMsgCount = 0;
    private static int putASortCount = 0;

    private static int fileIndex = 0;

    public static void init(){
        newAFc();
        newASortFc();
    }

    private static void newAFc() {
        try {
            aFc = new RandomAccessFile(Const.STORE_PATH + fileIndex + Const.M_A_FILE_SUFFIX, "rw").getChannel();
            aFcPool.add(aFc);
        } catch (FileNotFoundException e) {
            Utils.print(e.getMessage());
            System.exit(-1);
        }
    }


    private static void newASortFc() {
        try {
            aSortFc = new RandomAccessFile(Const.STORE_PATH + fileIndex + Const.M_A_SORT_FILE_SUFFIX, "rw").getChannel();
            aSortFcPool.add(aSortFc);
        } catch (FileNotFoundException e) {
            Utils.print(e.getMessage());
            System.exit(-1);
        }
    }

    public static void writeA(long a) {
        if (!aBuf.hasRemaining()) {
            flush(aBuf, aFc);
        }
        aBuf.putLong(a);


        //如果putAAndMsgCount等于FILE_STORE_MSG_NUM，也要刷盘，因为要切文件
        if (++putAAndMsgCount == Const.FILE_STORE_MSG_NUM) {
            flush(aBuf, aFc);

            fileIndex++;
            newAFc();
            putAAndMsgCount = 0;
        }
    }

    public static void writeASort(long a) {
        if (!aSortBuf.hasRemaining()) {
            flush(aSortBuf, aSortFc);
        }
        aSortBuf.putLong(a);

        //如果putAAndMsgCount等于FILE_STORE_MSG_NUM，也要刷盘，因为要切文件
        if (++putASortCount == Const.FILE_STORE_MSG_NUM) {
            flush(aSortBuf, aSortFc);
            newASortFc();

            putASortCount = 0;
        }
    }

    public static void flushEnd() {
        flush(aBuf, aFc);
        flush(aSortBuf, aSortFc);
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
    public static void readChunkA(int beginCount, long[] as, int readCount, ByteBuffer buf) {
        int fileIndex = beginCount / Const.FILE_STORE_MSG_NUM, filePos = (beginCount % Const.FILE_STORE_MSG_NUM) * Const.LONG_BYTES;
        readChunkAOrASort(as, readCount, buf, aFcPool.get(fileIndex), filePos);
    }

    public static void readChunkASort(int beginCount, long[] as, int readCount, ByteBuffer buf) {
        int fileIndex = beginCount / Const.FILE_STORE_MSG_NUM, filePos = (beginCount % Const.FILE_STORE_MSG_NUM) * Const.LONG_BYTES;
        readChunkAOrASort(as, readCount, buf, aSortFcPool.get(fileIndex), filePos);
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
        for (int i = 0; i <= fileIndex; i++) {
            try {
                aSize += aFcPool.get(i).size();
                aSortSize += aSortFcPool.get(i).size();
            } catch (IOException e) {
                Utils.print("FileManager log error " + e.getMessage());
                System.exit(-1);
            }
        }
        sb.append("fileNum:").append(fileIndex + 1).append(",aSize:")
                .append(aSize).append(",aSortSize:").append(aSortSize).append(",msgSize:").append(msgSize);
        sb.append("\n");
    }
}
