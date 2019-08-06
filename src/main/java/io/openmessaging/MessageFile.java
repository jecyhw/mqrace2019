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
    MemoryIndex memoryIndex = new MemoryIndex();
    int messageCount = 0;

    private ByteBuffer msgBuf = ByteBuffer.allocateDirect(Const.PUT_BUFFER_SIZE);

    static AtomicInteger idAllocator = new AtomicInteger(0);

    RandomAccessFile msgFile;
    FileChannel msgFc;

    boolean isTSequence = true;
    boolean isTEqual = false;
    long putMaxT = Long.MIN_VALUE;
    long putMinT = Long.MAX_VALUE;
    long putMaxA = Long.MIN_VALUE;
    long putMinA = Long.MAX_VALUE;
    long maxTInterval = Long.MIN_VALUE;

    int[] chCount = new int[256];
    boolean isByte = true;

    private Message prevMessage = new Message(0, 0, null);

    public MessageFile() {
        String fileId = String.valueOf(idAllocator.getAndIncrement());

        try {
            msgFile = new RandomAccessFile(Const.STORE_PATH + fileId + Const.MSG_FILE_SUFFIX, "rw");
            msgFc = msgFile.getChannel();
        } catch (FileNotFoundException e) {
            print("MessageFile constructor error " + e.getMessage());
        }

    }

    public void put(Message message) {
//        putStat(message);

        try {
            memoryIndex.put((int)message.getT(), (int)prevMessage.getT(), (int) message.getA(), (int) prevMessage.getA());
            writeMsg(message.getBody());
        } catch (IOException e) {
            print("func=put error t=" + message.getT() + " a=" + message.getA() + " msg=" + Utils.bytesToHex(message.getBody()) + " " + e.getMessage());
        }
        prevMessage = message;
    }

    public void get(long aMin, long aMax, long tMin, long tMax, GetItem getItem) {
        if (tMin <= tMax && aMin <= aMax) {
            MemoryRead memoryRead = getItem.memoryRead;
            MemoryGetItem minItem = getItem.minItem;
            MemoryGetItem maxItem = getItem.maxItem;
            memoryIndex.lowerBound((int)tMin, memoryRead, minItem);
            memoryIndex.upperBound((int)tMax, memoryRead, maxItem);

            int minPos = minItem.pos;
            int maxPos = maxItem.pos;
            if (minPos >= maxPos) {
                return;
            }
            ByteBuffer readBuf = getItem.buf;

            int[] as = getItem.as;
            int[] ts = memoryIndex.rangeT(minItem, maxItem, memoryRead);
            memoryIndex.rangeA(minItem, maxItem, memoryRead, as);

            readMsgs(minPos, maxPos, readBuf, as, ts, aMin, aMax, getItem);
        }
    }

    private void getStat(GetItem getItem, int count) {
        synchronized (MessageFile.class) {
            //统计命中的count
            getItem.maxCount = Math.max(getItem.maxCount, count);
            getItem.maxActualCount = Math.max(getItem.maxActualCount, getItem.messageSize);
        }
    }

    private void readMsgs(long minPos, long maxPos, ByteBuffer readBuf, int[] as, int[] ts, long aMin, long aMax, GetItem getItem) {
        List<Message> messages = getItem.messages;

        int messageSize = getItem.messageSize;
        int diffSize = ((int)(maxPos - minPos)) - (messages.size() - messageSize);
        if (diffSize > 0) {
            for (int i = 0; i < diffSize; i++) {
                messages.add(new Message(0, 0, new byte[34]));
            }
        }

        int i = 0;
        while (minPos < maxPos) {
            int readCount = Math.min((int)(maxPos - minPos), Const.MAX_MSG_CAPACITY) ;
            readBuf.position(0);
            readBuf.limit(readCount * Const.MSG_BYTES);
            readInBuf(minPos * Const.MSG_BYTES, readBuf, msgFc);
            readBuf.flip();


            while (readBuf.hasRemaining()) {
                int aVal = as[i];
                if (aVal >= aMin && aVal <= aMax) {
                    Message message = messages.get(messageSize++);
                    readBuf.get(message.getBody());
                    message.setA(aVal);
                    message.setT(ts[i]);

                } else {
                    readBuf.position(readBuf.position() + Const.MSG_BYTES);
                }
                i++;
            }

            minPos += readCount;
        }

        getItem.messageSize = messageSize;
    }

    public IntervalSum getAvgValue(long aMin, long aMax, long tMin, long tMax, GetItem getItem) {
        IntervalSum intervalSum = new IntervalSum();
        if (tMin <= tMax && aMin <= aMax) {
            long sum = 0;
            int count = 0;
            MemoryRead memoryRead = getItem.memoryRead;
            MemoryGetItem minItem = getItem.minItem;
            MemoryGetItem maxItem = getItem.maxItem;

            memoryIndex.lowerBound((int)tMin, memoryRead, minItem);
            memoryIndex.upperBound((int)tMax, memoryRead, maxItem);

            int minPos = minItem.pos;
            int maxPos = maxItem.pos;

            int[] as = getItem.as;
            if (minPos < maxPos) {
                int size = maxPos - minPos;
                int[] ts = getItem.as;
                memoryIndex.rangeA(minItem, maxItem, memoryRead, ts);
                for (int i = 0; i < size; i++) {
                    int aVal = as[i];
                    if (aVal >= aMin && aVal <= aMax) {
                        sum += aVal;
                        count++;
                    }
                }
                intervalSum.sum = sum;
                intervalSum.count = count;
            }
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

    private void putStat(Message message) {
        if (messageCount++ % Const.PRINT_MSG_INTERVAL == 0) {
            print("func=putStat t=" + message.getT() + " a=" + message.getA() + " msg=" + Utils.bytesToHex(message.getBody()));
        }
        putMaxA = Math.max(putMaxA, message.getA());
        putMinA = Math.min(putMinA, message.getA());
        putMaxT = Math.max(putMaxT, message.getT());
        putMinT = Math.min(putMinT, message.getT());

        for (byte b : message.getBody()) {
            //b可能有负数
            int i = b + 128;
            if (i >= 0 && i < 256) {
                chCount[i]++;
            } else {
                if (isByte) {
                    isByte = false;
                    print("func=putStat range_over byte=" + i);
                }
            }
        }

        if (prevMessage != null) {
            if (isTSequence && message.getT() < prevMessage.getT()) {
                print("func=putStat t is not_sequence t=" + message.getT() + " prevT=" + prevMessage.getT());
                isTSequence = false;
            }
            if (!isTEqual && message.getT() == prevMessage.getT()) {
                print("func=putStat t is equal t=" + message.getT());
                isTEqual = true;
            }
            maxTInterval = Math.max(maxTInterval, message.getT() - prevMessage.getT());
        }
    }

    private void writeInt(FileChannel fc, ByteBuffer buf, int a) throws IOException {
        if (buf.remaining() < Const.LONG_BYTES) {
            flush(fc, buf);
        }
        buf.putInt(a);
    }

    private void writeShort(FileChannel fc, ByteBuffer buf, short a) throws IOException {
        if (buf.remaining() < Const.A_BYTES) {
            flush(fc, buf);
        }
        buf.putShort(a);
    }

    private void writeMsg(byte[] msg) throws IOException {
        if (msgBuf.remaining() < msg.length) {
            flush(msgFc, msgBuf);
        }
        msgBuf.put(msg);
    }

    private void flush(FileChannel fc, ByteBuffer buf) throws IOException {
        buf.flip();
        while (buf.hasRemaining()) {
            fc.write(buf);
        }
        buf.clear();
    }

    public void close() {
        try {
            if (msgFc != null) {
                msgFc.close();
                msgFile.close();
            }
        } catch (IOException e) {
            print("func=close error " + e.getMessage());
        }
    }

    public void flush() {
        try {
            memoryIndex.flush();
            flush(msgFc, msgBuf);

            msgBuf = null;
        } catch (IOException e) {
            print("func=flush error " + e.getMessage());
        }

    }
}
