package io.openmessaging;

import org.iq80.snappy.Snappy;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.Utils.print;

/**
 * Created by yanghuiwei on 2019-07-26
 */
public class MessageFile {
    private static AtomicInteger idAllocator = new AtomicInteger(0);

    //直接压缩到这个字节数组上
    private final byte[] msgData = new byte[Const.PUT_BUFFER_SIZE];
    private int msgDataPos = 0;
    private final ByteBuffer msgBuf = ByteBuffer.wrap(msgData);
    private RandomAccessFile msgFile;
    private long msgFileSize = 0;
    private FileChannel msgFc;
    private final byte[] uncompressMsgData = new byte[Const.COMPRESS_MSG_SIZE];
    private int uncompressMsgDataPos = 0;

    private final ByteBuffer aBuf = ByteBuffer.allocateDirect(Const.PUT_BUFFER_SIZE);
    private RandomAccessFile aFile;
    private FileChannel aFc;

    private final static AtomicInteger indexBufCounter = new AtomicInteger();
    private final static AtomicInteger tBufCounter = new AtomicInteger(0);
    private final long[] tArr = new long[Const.INDEX_ELE_LENGTH];
    private final int[] offsetArr = new int[Const.INDEX_ELE_LENGTH];
    private final long[] msgOffsetArr = new long[Const.INDEX_ELE_LENGTH];


    //put计数
    private int putCount = 0;

    //索引内存数组
    //indexBufs存的元素个数
    private int indexBufEleCount = 0;

    //t的相对值存储的内存
    private final ByteBuffer memory = ByteBuffer.allocateDirect(Const.MEMORY_BUFFER_SIZE);
    //已使用的比特位数
    private int putBitLength = 0;

    private Message prevMessage = new Message(0, 0, null);

    public MessageFile() {
        try {
            int fileId = idAllocator.getAndIncrement();
            msgFile = new RandomAccessFile(Const.STORE_PATH + fileId + Const.MSG_FILE_SUFFIX, "rw");
            msgFc = msgFile.getChannel();

            aFile = new RandomAccessFile(Const.STORE_PATH + fileId + Const.A_FILE_SUFFIX, "rw");
            aFc = aFile.getChannel();
        } catch (FileNotFoundException e) {
            print("MessageFile constructor error " + e.getMessage());
        }

    }

    public void put(Message message) {
        long t = message.getT();
        //比如对于1 2 3 4 5 6，间隔为2，会存 1 3 5
        if (putCount % Const.INDEX_INTERVAL == 0) {
            int pos = indexBufEleCount;
            //每隔INDEX_INTERVAL记录t（在indexBuf中记录了t就不会在memory中记录）
            tArr[pos] = t;
            //下一个t的起始位置，先写在哪个块中，再写块的便宜位置
            offsetArr[pos] = putBitLength;
            if (uncompressMsgDataPos == Const.COMPRESS_MSG_SIZE) {
                compressMsgBody();
            }
            //需要先压缩在赋值
            msgOffsetArr[pos] = msgFileSize + msgDataPos;

            indexBufEleCount++;
        } else {
            int diffT = (int)(t - prevMessage.getT());
            putBitLength = VariableUtils.putUnsigned(memory, putBitLength, diffT);
        }
        putCount++;

        System.arraycopy(message.getBody(), 0, uncompressMsgData, uncompressMsgDataPos, Const.MSG_BYTES);
        uncompressMsgDataPos += Const.MSG_BYTES;

        writeA(message.getA());
        prevMessage = message;
    }

    private void compressMsgBody() {
        //判断是否要刷落盘
        if (msgDataPos + Const.COMPRESS_MSG_SIZE > Const.PUT_BUFFER_SIZE) {
            flushMsg();
            msgFileSize += msgDataPos;
            msgDataPos = 0;
        }
        msgDataPos += Snappy.compress(uncompressMsgData, 0, Const.COMPRESS_MSG_SIZE, msgData, msgDataPos);
        uncompressMsgDataPos = 0;
    }

    private void flushMsg() {
        msgBuf.limit(msgDataPos);
        try {
            while (msgBuf.hasRemaining()) {
                msgFc.write(msgBuf);
            }
        } catch (Exception e) {
            Utils.print("func=flush error");
        }
        msgBuf.position(0);
    }

    private void writeA(long a) {
        if (aBuf.remaining() < Const.LONG_BYTES) {
            flush(aFc, aBuf);
        }
        aBuf.putLong(a);
    }

    public List<Message> get(long aMin, long aMax, long tMin, long tMax, GetItem getItem) {
        if (tMin <= tMax && aMin <= aMax) {
            int minPos = firstLessInPrimaryIndex(tMin);
            int maxPos = firstGreatInPrimaryIndex(tMax);

            if (minPos >= maxPos) {
                return Collections.emptyList();
            }

            long[] ts = getItem.ts;
            int tLen = rangePosInPrimaryIndex(minPos, maxPos, ts);

            int realMinPos = minPos * Const.INDEX_INTERVAL;
            long[] as = getItem.as;
            readAArray(realMinPos, realMinPos + tLen, getItem.buf, as);
            //minPos，maxPos是在主内存索引的位置
            return readMsgs(minPos, maxPos, getItem, as, ts, tLen, aMin, aMax, tMin, tMax);
        } else {
            return Collections.emptyList();
        }
    }

    private List<Message> readMsgs(int minPos, int maxPos, GetItem getItem, long[] as, long[] ts, int tLen, long aMin, long aMax, long tMin, long tMax) {
        ByteBuffer readBuf = getItem.buf;

        //计算要读取的msg
        long startOffset = msgOffsetArr[minPos];
        int len = (int)(msgOffsetArr[maxPos + 1] - startOffset);
        readBuf.position(0);
        readBuf.limit(len);
        readInBuf(startOffset, readBuf, msgFc);

        byte[] compressMsgData = readBuf.array();
        int compressMsgDataPos = 0, compressSize;
        byte[] uncompressMsgData = getItem.uncompressMsgData;

        List<Message> messages = new ArrayList<>();

        int pos = 0;
        while (minPos < maxPos) {
            compressSize = (int)(msgOffsetArr[minPos + 1] - msgOffsetArr[minPos]);
            Snappy.uncompress(compressMsgData, compressMsgDataPos, compressSize, uncompressMsgData, 0);
            compressMsgDataPos += compressSize;

            for (int i = 0, uncompressMsgDataPos = 0; i < Const.INDEX_INTERVAL && pos < tLen; i++, uncompressMsgDataPos += Const.MSG_BYTES) {
                long a = as[pos], t = ts[pos];
                if (a >= aMin && a <= aMax && t >= tMin && t <= tMax) {
                    byte[] body = new byte[Const.MSG_BYTES];
                    System.arraycopy(uncompressMsgData, uncompressMsgDataPos, body, 0, Const.MSG_BYTES);
                    messages.add(new Message(a, t, body));
                }
                pos++;
            }

            minPos++;
        }
        return messages;
    }

    public void getAvgValue(long aMin, long aMax, long tMin, long tMax, IntervalSum intervalSum, GetItem getItem) {
        if (tMin <= tMax && aMin <= aMax) {
            int minPos = firstLessInPrimaryIndex(tMin);
            int maxPos = firstGreatInPrimaryIndex(tMax);
            if (minPos >= maxPos) {
                return;
            }

            long[] ts = getItem.ts;
            int tLen = rangePosInPrimaryIndex(minPos, maxPos, ts);

            int realMinPos = minPos * Const.INDEX_INTERVAL;
            long[] as = getItem.as;
            readAArray(realMinPos, realMinPos + tLen, getItem.buf, as);

            long sum = 0;
            int count = 0;
            for (int i = 0; i < tLen; i++) {
                long a = as[i], t = ts[i];
                if (t != a) {
                    System.err.println(t + ":" + a);
                }
                if (a >= aMin && a <= aMax && t >= tMin && t <= tMax) {
                    sum += a;
                    count++;
                }
            }

            intervalSum.count +=  count;
            intervalSum.sum += sum;
        }
    }

    private void readAArray(int minPos, int maxPos, ByteBuffer readBuf, long[] as) {
        int cnt = 0;
        while (minPos < maxPos) {
            int readCount = Math.min((maxPos - minPos), Const.MAX_LONG_CAPACITY);
            readBuf.position(0);
            readBuf.limit(readCount * Const.LONG_BYTES);
            readInBuf(minPos * Const.LONG_BYTES, readBuf, aFc);
            readBuf.flip();

            while (readBuf.hasRemaining()) {
                as[cnt++] = readBuf.getLong();
            }
            minPos += readCount;
        }
    }

    private void readInBuf(long pos, ByteBuffer bb, FileChannel fc) {
        try {
            while (bb.hasRemaining()) {
                int read = fc.read(bb, pos);
                pos += read;
            }
        } catch (IOException e) {
            //出现异常返回最大值，最终查找到的message列表就为空
            print("func=readInBuf error pos=" + pos + " " + e.getMessage());
        }
    }

    private void flush(FileChannel fc, ByteBuffer buf) {
        buf.flip();
        try {
            while (buf.hasRemaining()) {
                fc.write(buf);
            }
        } catch (Exception e) {
            Utils.print("func=flush error");
        }
        buf.clear();
    }

    public void flush() {
        flushMsg();
        msgOffsetArr[indexBufEleCount] = msgFileSize += msgDataPos;

        flush(aFc, aBuf);

        try {
            Utils.print("MemoryIndex func=flush indexBuf:" + indexBufCounter.get() + " tBuf:" + tBufCounter.get() + " indexBufEleCount:" + indexBufEleCount
                    + " putCount:" + putCount + " putBitLength:" + putBitLength / 8 + " aFilSize:" + aFile.length() + " compressMsgFileSize:" + msgFile.length() + ":" + msgFileSize + " msgFileSize:" + ((long)putCount * Const.MSG_BYTES));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param minPos >= 在PrimaryIndex的开始位置
     * @param maxPos < 在PrimaryIndex的结束位置
     * @return 返回读取的条数
     */
    public int rangePosInPrimaryIndex(int minPos, int maxPos, long[] destT) {
        //数据可能会存在多个块中
        int nextOffset, putBitLength = this.putBitLength;
        int destOffset = 0;

        int[] diffT = new int[1];
        while (minPos < maxPos) {
            //得到索引内存中这个位置的t值
            destT[destOffset] = tArr[minPos];
            destOffset++;

            //得到这个t的下一个t在内存块的位置
            nextOffset = offsetArr[minPos];
            if (nextOffset >= putBitLength) {
                return destOffset;
            }

            //从变长编码内存中读
            for (int k = 1 ; k < Const.INDEX_INTERVAL; k++) {
                nextOffset = VariableUtils.getUnsigned(memory, nextOffset, diffT, 0);
                destT[destOffset] = destT[destOffset - 1] + diffT[0];

                destOffset++;

                if (nextOffset >= putBitLength) {
                    //说明读完了
                    return destOffset;
                }
            }
            minPos++;
        }
        return destOffset;
    }


    public int firstGreatInPrimaryIndex(long val) {
        int low = 0, high = indexBufEleCount, mid;
        while(low < high){
            mid = low + (high - low) / 2;

            if(tArr[mid] > val) {
                high = mid;
            }
            else {
                low = mid + 1;
            }
        }
        return low;
    }

    /**
     * 查找第一个小于val的数字位置，如果没有将返回PrimaryIndex的第一个位置
     */
    public int firstLessInPrimaryIndex(long val) {
        int low = 0, high = indexBufEleCount, mid;

        //先找第一个大于等于val的位置，减1就是第一个小于val的位置
        while (low < high) {
            mid = low + (high - low) / 2;

            if (val > tArr[mid]) {
                low = mid + 1;
            }
            else {
                high = mid;
            }
        }
        return low == 0 ? low : low - 1;
    }
}
