package io.openmessaging;

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
    static AtomicInteger idAllocator = new AtomicInteger(0);

    MemoryIndex memoryIndex = new MemoryIndex();
    private ByteBuffer msgBuf = ByteBuffer.allocateDirect(Const.PUT_BUFFER_SIZE);
    RandomAccessFile msgFile;
    FileChannel msgFc;

    private Message prevMessage = new Message(0, 0, null);

    public MessageFile() {
        try {
            msgFile = new RandomAccessFile(Const.STORE_PATH + idAllocator.getAndIncrement() + Const.MSG_FILE_SUFFIX, "rw");
            msgFc = msgFile.getChannel();
        } catch (FileNotFoundException e) {
            print("MessageFile constructor error " + e.getMessage());
        }

    }

    public void put(Message message) {
        memoryIndex.put((int)message.getT(), (int)prevMessage.getT(), (int) message.getA(), (int) prevMessage.getA());
        writeMsg(message.getBody());
        prevMessage = message;
    }

    public List<Message> get(int aMin, int aMax, int tMin, int tMax, GetItem getItem) {
        if (tMin <= tMax && aMin <= aMax) {
            int minPos = memoryIndex.firstLessInPrimaryIndex(tMin);
            int maxPos = memoryIndex.firstGreatInPrimaryIndex(tMax);

            if (minPos >= maxPos) {
                return new ArrayList<>();
            }

            ByteBuffer readBuf = getItem.buf;

            int[] ts = getItem.ts;
            int[] as = getItem.as;
            int tLen = memoryIndex.rangePosInPrimaryIndex(minPos, maxPos, ts, as);
            //minPos，maxPos是在主内存索引的位置
            return readMsgs(minPos * Const.INDEX_INTERVAL, readBuf, as, ts, tLen, aMin, aMax, tMin, tMax);
        } else {
            return Collections.emptyList();
        }
    }

    private List<Message> readMsgs(long minPos, ByteBuffer readBuf, int[] as, int[] ts, int tLen, int aMin, int aMax, int tMin, int tMax) {
        List<Message> messages = new ArrayList<>();
        //从后往前过滤
        tLen--;
        while (tLen >= 0 && ts[tLen] > tMax) {
            tLen--;
        }

        //从前往后过滤
        int s = 0;
        while (s <= tLen && ts[s] < tMin) {
            s++;
            //注意minPos也要跟着自增
            minPos++;
        }

        tLen++;
        while (s < tLen) {
            int readCount = Math.min((tLen - s), Const.MAX_MSG_CAPACITY) ;
            readBuf.position(0);
            readBuf.limit(readCount * Const.MSG_BYTES);
            readInBuf(minPos * Const.MSG_BYTES, readBuf, msgFc);
            readBuf.flip();
            minPos += readCount;

            while (readBuf.hasRemaining()) {
                int a = as[s];
                if (a >= aMin && a <= aMax) {
                    Message message = MessageCacheShare.get();
                    readBuf.get(message.getBody());
                    message.setA(a);
                    message.setT(ts[s]);
                    messages.add(message);
                } else {
                    readBuf.position(readBuf.position() + Const.MSG_BYTES);
                }
                s++;
            }
        }
        return messages;
    }

    public IntervalSum getAvgValue(int aMin, int aMax, int tMin, int tMax, IntervalSum intervalSum) {
        if (tMin <= tMax && aMin <= aMax) {
            int minPos = memoryIndex.firstLessInPrimaryIndex(tMin);
            memoryIndex.sum(minPos, aMin, aMax, tMin, tMax, intervalSum);
        }
        return intervalSum;
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

    private void writeMsg(byte[] msg) {
        if (msgBuf.remaining() < msg.length) {
            flush(msgFc, msgBuf);
        }
        msgBuf.put(msg);
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
        flush(msgFc, msgBuf);
        memoryIndex.flush();
    }
}
