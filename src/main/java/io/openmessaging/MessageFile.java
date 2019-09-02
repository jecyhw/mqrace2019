package io.openmessaging;

import io.openmessaging.codec.*;
import io.openmessaging.model.GetMsgItem;
import io.openmessaging.util.ArrayUtils;
import io.openmessaging.util.Utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.util.Utils.print;

/**
 * Created by yanghuiwei on 2019-07-26
 */
public class MessageFile {
    private static final AtomicInteger idAllocator = new AtomicInteger(0);
    private final ByteBuffer tBuf = ByteBuffer.allocate(Const.MEMORY_BUFFER_SIZE);
    private TEncoder tEncoder = new TEncoder(tBuf);
    private final long[] tArr = new long[Const.INDEX_ELE_LENGTH];
    private final int[] tOffsetArr = new int[Const.INDEX_ELE_LENGTH];
    private long lastT;

    //直接压缩到这个字节数组上
    private final ByteBuffer msgBuf = ByteBuffer.allocate(Const.PUT_BUFFER_SIZE);
    private FileChannel msgFc;

    private final ByteBuffer aBuf = ByteBuffer.allocate(Const.PUT_BUFFER_SIZE);
    private FileChannel aFc;

    //put计数
    int putCount = 0;

    //索引内存数组
    //indexBufs存的元素个数
    private int blockNums = 0;


    public MessageFile() {
        int fileId = idAllocator.getAndIncrement();
        try {
            msgFc = new RandomAccessFile(Const.STORE_PATH + fileId + Const.MSG_FILE_SUFFIX, "rw").getChannel();
            aFc = new RandomAccessFile(Const.STORE_PATH + fileId + Const.A_FILE_SUFFIX, "rw").getChannel();
        } catch (FileNotFoundException e) {
            print("MessageFile constructor error " + e.getMessage());
        }

    }

    public final void put(Message message) {
        long t = message.getT(), a = message.getA();
        //比如对于1 2 3 4 5 6，间隔为2，会存 1 3 5
        if (putCount % Const.INDEX_INTERVAL == 0) {
            int blockNum = blockNums;

            //每隔INDEX_INTERVAL记录t（在indexBuf中记录了t就不会在memory中记录）
            tArr[blockNum] = t;
            //下一个t的起始位置，先写在哪个块中，再写块的便宜位置
            tOffsetArr[blockNum] = tEncoder.getBitPosition();
            tEncoder.resetDelta();

            blockNums++;
        } else {
            tEncoder.encode((int) (t - lastT));
        }

        writeA(a);
        writeMsg(message.getBody());

        putCount++;
        lastT = t;

        if (putCount % (1024 * 1024 * 8) == 0) {
            Utils.print("putCount:" + putCount);
        }
    }

    private void writeMsg(byte[] body) {
        if (msgBuf.remaining() - Const.MSG_BYTES < 0) {
            flush(msgFc, msgBuf);
        }
        msgBuf.put(body);
    }

    private void writeA(long a) {
        if (!aBuf.hasRemaining()) {
            flush(aFc, aBuf);
        }
        aBuf.putLong(a);
    }

    public final void get(long aMin, long aMax, long tMin, long tMax, GetMsgItem getItem) {
        ByteBuffer tBuf = this.tBuf.duplicate();
        if (tMin <= tMax && aMin <= aMax) {
            int minPos = ArrayUtils.findFirstLessThanIndex(tArr, tMin, 0, blockNums);
            int maxPos = ArrayUtils.findFirstGreatThanIndex(tArr, tMax, 0, blockNums);

            if (minPos >= maxPos) {
                return;
            }

            long[] ts = getItem.ts;
            int tLen = rangePosInPrimaryIndex(minPos, maxPos, ts, getItem, tBuf);

            long[] as = getItem.as;
            readAArray(minPos * Const.INDEX_INTERVAL, tLen, getItem.readBuf, as);
            //minPos，maxPos是在主内存索引的位置
            readMsgs(minPos * Const.INDEX_INTERVAL, getItem, as, ts, tLen, aMin, aMax, tMin, tMax);
        }
    }

    /**
     *
     * @param minPos >= 在PrimaryIndex的开始位置
     * @param maxPos < 在PrimaryIndex的结束位置
     * @param tBuf
     * @return 返回读取的条数
     */
    private int rangePosInPrimaryIndex(int minPos, int maxPos, long[] destT, GetMsgItem getItem, ByteBuffer tBuf) {
        TDecoder decoder = getItem.tDecoder;
        int lastInterval = 0;
        if (maxPos == blockNums) {
            maxPos--;
            int destOffset = (maxPos - minPos) * Const.INDEX_INTERVAL;
            destT[destOffset] = tArr[maxPos];
            //需要注意，如果putCount刚好是Const.INDEX_INTERVAL整数倍，还是需要从编码中读取Const.INDEX_INTERVAL-1个数
            lastInterval = (putCount - 1) % Const.INDEX_INTERVAL;
            decoder.decode(tBuf, destT, destOffset + 1,  tOffsetArr[maxPos], lastInterval);
            lastInterval = lastInterval + 1;
        }

        int destOffset = 0;
        while (minPos < maxPos) {
            destT[destOffset] = tArr[minPos];
            decoder.decode(tBuf, destT, destOffset + 1, tOffsetArr[minPos], Const.INDEX_INTERVAL - 1);
            destOffset += Const.INDEX_INTERVAL;
            minPos++;
        }
        return destOffset + lastInterval;

    }

    private void readMsgs(long fileStartPos, GetMsgItem getItem, long[] as, long[] ts, int len, long aMin, long aMax, long tMin, long tMax) {
        ByteBuffer readBuf = getItem.readBuf;

        readBuf.position(0);
        readBuf.limit(len * Const.MSG_BYTES);
        //必须是一次性拿
        readInBuf(fileStartPos * Const.MSG_BYTES, readBuf, msgFc);
        readBuf.position(0);

        List<Message> messages = getItem.messages;

        for (int i = 0, pos = Const.MSG_BYTES; i < len; i++, pos += Const.MSG_BYTES) {
            //中间只需要判断a
            long a = as[i], t = ts[i];
            //先读区间的第一个数
            if (a >= aMin && a <= aMax && t >= tMin && t <= tMax) {
                byte[] body = new byte[Const.MSG_BYTES];
                readBuf.get(body);
                messages.add(new Message(a, t, body));
            } else {
                readBuf.position(pos);
            }
        }
    }

    private void readAArray(int fileStartPos, int len, ByteBuffer readBuf, long[] as) {
        readAFromFile(fileStartPos  * Const.LONG_BYTES, len, readBuf, as, 0);
    }

    private void readAFromFile(int filePos, int readLen, ByteBuffer readBuf, long[] as, int aPos) {
        readBuf.position(0);
        readBuf.limit(readLen * 8);
        //必须是一次性拿
        readInBuf(filePos, readBuf, aFc);

        readBuf.position(0);
        for (int i = 0; i < readLen; i++, aPos++) {
            as[aPos] = readBuf.getLong();
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
        write(fc, buf);
        buf.clear();
    }

    private void write(FileChannel fc, ByteBuffer buf) {
        try {
            while (buf.hasRemaining()) {
                fc.write(buf);
            }
        } catch (Exception e) {
            Utils.print("func=createIndex error");
        }
    }


    public final void flush() {
        //最后一块进行压缩
        flush(msgFc, msgBuf);

        flush(aFc, aBuf);
        tEncoder.flushAndClear();

        try {
            Utils.print("MemoryIndex func=createIndex " + " blockNums:" + blockNums
                    + " putCount:" + putCount + " aFilSize:" + aFc.size() + " compressMsgFileSize:" + msgFc.size()
                    + " msgFileSize:" + ((long)putCount * Const.MSG_BYTES)
                    + " bitPos:" + tOffsetArr[blockNums - 1] / 8
                    + " bufSize:"+ tBuf.limit());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Iterator iterator() {
        return new Iterator();
    }


    class Iterator {
        int readBlockNums = 0;
        TDecoder tDecoder = new TDecoder();

        boolean hasNext() {
            return readBlockNums < blockNums;
        }

        int nextTAndA(long[] t, long[] a, ByteBuffer aBuf) {
            int readCount = readBlockNums == blockNums - 1 ? (putCount - 1) % Const.INDEX_INTERVAL : Const.INDEX_INTERVAL - 1;
            t[0] = tArr[readBlockNums];
            tDecoder.decode(tBuf, t, 1, tOffsetArr[readBlockNums], readCount);

            readCount++;

            aBuf.position(0);
            aBuf.limit(readCount * Const.LONG_BYTES);
            readInBuf(readBlockNums * Const.INDEX_INTERVAL * Const.LONG_BYTES, aBuf, aFc);
            aBuf.position(0);
            for (int i = 0; i < readCount; i++) {
                a[i] = aBuf.getLong();
            }

            readBlockNums++;
            return readCount;
        }
    }
}
