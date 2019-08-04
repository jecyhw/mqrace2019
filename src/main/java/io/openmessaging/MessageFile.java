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
    private ByteBuffer aBuf = ByteBuffer.allocateDirect(Const.PUT_BUFFER_SIZE);

    static AtomicInteger idAllocator = new AtomicInteger(0);

    RandomAccessFile msgFile;
    FileChannel msgFc;
    RandomAccessFile aFile;
    FileChannel aFc;

    boolean isTSequence = true;
    boolean isTEqual = false;
    long putMaxT = Long.MIN_VALUE;
    long putMinT = Long.MAX_VALUE;
    long putMaxA = Long.MIN_VALUE;
    long putMinA = Long.MAX_VALUE;
    long maxTInterval = Long.MIN_VALUE;

    int[] chCount = new int[256];
    boolean isByte = true;

    int[] aIntervals = new int[70002];
    private Message prevMessage = new Message(0, 0, null);

    public MessageFile() {
        String fileId = String.valueOf(idAllocator.getAndIncrement());

        try {
            aFile = new RandomAccessFile(Const.STORE_PATH + fileId + Const.A_FILE_SUFFIX, "rw");
            aFc = aFile.getChannel();

            msgFile = new RandomAccessFile(Const.STORE_PATH + fileId + Const.MSG_FILE_SUFFIX, "rw");
            msgFc = msgFile.getChannel();
        } catch (FileNotFoundException e) {
            print("MessageFile constructor error " + e.getMessage());
        }

    }

    public void put(Message message) {
//        putStat(message);

        try {
            memoryIndex.put((int)message.getT(), (int)prevMessage.getT());
            writeShort(aFc, aBuf, (short)(message.getT()-message.getA()+Const.A_OFFSET) );
            writeMsg(message.getBody());
        } catch (IOException e) {
            print("func=put error t=" + message.getT() + " a=" + message.getA() + " msg=" + Utils.bytesToHex(message.getBody()) + " " + e.getMessage());
        }
        prevMessage = message;
    }

    public List<Message> get(long aMin, long aMax, long tMin, long tMax, GetItem getItem, ArrBuffer arrBuffer) {
        if (tMin <= tMax && aMin <= aMax) {
            MemoryRead memoryRead = getItem.memoryRead;
            MemoryGetItem minItem = getItem.minItem;
            MemoryGetItem maxItem = getItem.maxItem;
            memoryIndex.lowerBound((int)tMin, memoryRead, minItem);
            memoryIndex.upperBound((int)tMax, memoryRead, maxItem);

            int minPos = minItem.pos;
            int maxPos = maxItem.pos;
            if (minPos >= maxPos) {
                return new ArrayList<>();
            }
            ByteBuffer readBuf = getItem.buf;

            readAArray(minPos, maxPos, readBuf, aFc, arrBuffer.getAs());
            int[] ts = memoryIndex.range(minItem, maxItem, memoryRead);

            List<Message> messages = readMsgs(minPos, maxPos, readBuf, arrBuffer.getAs(), ts, aMin, aMax);
            getStat(getItem, maxPos - minPos, messages.size());
            return messages;
        } else {
            return Collections.emptyList();
        }
    }

    private void getStat(GetItem getItem, int count, int actualCount) {
        synchronized (MessageFile.class) {
            //统计命中的count
            getItem.maxCount = Math.max(getItem.maxCount, count);
            getItem.maxActualCount = Math.max(getItem.maxActualCount, actualCount);
        }
    }

    private List<Message> readMsgs(long minPos, long maxPos, ByteBuffer readBuf, int[] as, int[] ts, long aMin, long aMax) {
        List<Message> messages = new ArrayList<>();
        int i = 0;
        while (minPos < maxPos) {
            int readCount = Math.min((int)(maxPos - minPos), Const.MAX_MSG_CAPACITY) ;
            readBuf.position(0);
            readBuf.limit(readCount * Const.MSG_BYTES);
            readInBuf(minPos * Const.MSG_BYTES, readBuf, msgFc);
            readBuf.flip();

            while (readBuf.hasRemaining()) {
                int aVal = ts[i]-as[i]+Const.A_OFFSET;
                if (aVal >= aMin && aVal <= aMax) {
                    Message message = MessageCacheShare.get();
                    readBuf.get(message.getBody());
                    message.setA(aVal);
                    message.setT(ts[i]);
                    messages.add(message);
                } else {
                    readBuf.position(readBuf.position() + Const.MSG_BYTES);
                }
                i++;
            }

            minPos += readCount;
        }
        return messages;
    }

    private int[] readArray(long minPos, long maxPos, ByteBuffer readBuf, FileChannel fc) {
        int[] arr = new int[(int)(maxPos - minPos)];
        int cnt = 0;
        while (minPos < maxPos) {
            int readCount = Math.min((int)(maxPos - minPos), Const.MAX_LONG_CAPACITY);
            readBuf.position(0);
            readBuf.limit(readCount * Const.LONG_BYTES);
            readInBuf(minPos * Const.LONG_BYTES, readBuf, fc);
            readBuf.flip();

            while (readBuf.hasRemaining()) {
                arr[cnt++] = readBuf.getInt();
            }
            minPos += readCount;
        }
        return arr;
    }
    private int readAArray(long minPos, long maxPos, ByteBuffer readBuf, FileChannel fc, int[] arr) {
        int cnt = 0;
        while (minPos < maxPos) {
            int readCount = Math.min((int)(maxPos - minPos), Const.MAX_LONG_CAPACITY);
            readBuf.position(0);
            readBuf.limit(readCount * Const.A_BYTES);
            readInBuf(minPos * Const.A_BYTES, readBuf, fc);
            readBuf.flip();

            while (readBuf.hasRemaining()) {
                arr[cnt++] = (int)readBuf.getShort();
            }
            minPos += readCount;
        }
        return cnt;
    }

    public IntervalSum getAvgValue(long aMin, long aMax, long tMin, long tMax, GetItem getItem, ArrBuffer arrBuffer) {
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

            if (minPos < maxPos) {
                ByteBuffer readBuf = getItem.buf;
                int[] as = arrBuffer.getAs();
                int[] ts = memoryIndex.range(minItem, maxItem, memoryRead);
                int size = readAArray(minPos, maxPos, readBuf, aFc, as);
                for (int i = 0; i < size; i++) {
                    int aVal = ts[i] - as[i] + Const.A_OFFSET;
                    if (aVal >= aMin && aVal <= aMax) {
                        sum += aVal;
                        count++;
                    }
                }
                intervalSum.sum = sum;
                intervalSum.count = count;

                getStat(getItem, (maxPos - minPos), count);
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
            int aInterval = (int)(message.getA() - prevMessage.getA()) + 35000;
            if (aInterval < 0) {
                aIntervals[70001]++;
            } else if (aInterval > 70000){
                aIntervals[70000]++;
            } else {
                aIntervals[aInterval]++;
            }
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

                aFile.close();
                aFc.close();
            }
        } catch (IOException e) {
            print("func=close error " + e.getMessage());
        }
    }

    public void flush() {
        try {
            memoryIndex.flush();
            flush(aFc, aBuf);
            flush(msgFc, msgBuf);

            aBuf = null;
            msgBuf = null;
        } catch (IOException e) {
            print("func=flush error " + e.getMessage());
        }

    }
}
