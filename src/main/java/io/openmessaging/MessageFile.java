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
    int messageCount = 0;
    private ByteBuffer msgBuf = ByteBuffer.allocateDirect(Const.BUFFER_SIZE);
    private ByteBuffer tBuf = ByteBuffer.allocateDirect(Const.BUFFER_SIZE);
    private ByteBuffer aBuf = ByteBuffer.allocateDirect(Const.BUFFER_SIZE);

    static AtomicInteger idAllocator = new AtomicInteger(0);

    RandomAccessFile msgFile;
    FileChannel msgFc;
    RandomAccessFile tFile;
    FileChannel tFc;
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

    private Message prevMessage;

    public MessageFile() {
        String fileId = String.valueOf(idAllocator.getAndIncrement());

        try {
            tFile = new RandomAccessFile(Const.STORE_PATH + fileId + Const.T_FILE_SUFFIX, "rw");
            tFc = tFile.getChannel();

            aFile = new RandomAccessFile(Const.STORE_PATH + fileId + Const.A_FILE_SUFFIX, "rw");
            aFc = aFile.getChannel();

            msgFile = new RandomAccessFile(Const.STORE_PATH + fileId + Const.MSG_FILE_SUFFIX, "rw");
            msgFc = msgFile.getChannel();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            print("MessageFile constructor error");
        }

    }

    public void put(Message message) {
        putStat(message);

        try {
            writeLong(tFc, tBuf, message.getT());
            writeLong(aFc, aBuf, message.getA());
            writeMsg(message.getBody());
        } catch (IOException e) {
            e.printStackTrace();
            print("func=put error t=" + message.getT() + " a=" + message.getA() + " msg=" + Utils.bytesToHex(message.getBody()));
        }
    }

    public List<Message> get(long aMin, long aMax, long tMin, long tMax, GetItem getItem) {
        if (tMin <= tMax && aMin <= aMax) {
            try {
                ByteBuffer keyBuf = getItem.keyBuf;
                long minPos = lowerBound(tMin, keyBuf);
                long maxPos = upperBound(tMax, keyBuf);
                if (minPos >= maxPos) {
                    return Collections.emptyList();
                }

                ByteBuffer readBuf = getItem.buf;
                long[] as = readLongArray(minPos, maxPos, readBuf, aFc);
                long[] ts = readLongArray(minPos, maxPos, readBuf, tFc);

                List<Message> messages = readMsgs(minPos, maxPos, readBuf, as, ts, aMin, aMax);
                getStat(getItem, (int)(maxPos - minPos), messages.size());
                return messages;
            } catch (Exception e) {
                e.printStackTrace();
                print("func=get error aMin=" + aMin + " aMax=" + aMax + " tMin" + tMin + " tMax=" + tMax);
                return Collections.emptyList();
            }
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

    private List<Message> readMsgs(long minPos, long maxPos, ByteBuffer readBuf, long[] as, long[] ts, long aMin, long aMax) {
        List<Message> messages = new ArrayList<>();
        int i = 0;
        while (minPos < maxPos) {
            int readCount = Math.min((int)(maxPos - minPos), Const.MAX_MSG_CAPACITY) ;
            readBuf.position(0);
            readBuf.limit(readCount * Const.MSG_BYTES);
            readInBuf(minPos * Const.MSG_BYTES, readBuf, msgFc);

            readBuf.flip();
            while (readBuf.hasRemaining()) {
                if (as[i] >= aMin && as[i] <= aMax) {
                    byte[] msg = new byte[Const.MSG_BYTES];
                    readBuf.get(msg);
                    messages.add(new Message(as[i], ts[i], msg));
                } else {
                    readBuf.position(readBuf.position() + Const.MSG_BYTES);
                }
                i++;
            }

            minPos += readCount;
        }
        return messages;
    }

    private long[] readLongArray(long minPos, long maxPos, ByteBuffer readBuf, FileChannel fc) {
        long[] arr = new long[(int)(maxPos - minPos)];
        int cnt = 0;
        while (minPos < maxPos) {
            int readCount = Math.min((int)(maxPos - minPos), Const.MAX_LONG_CAPACITY);
            readBuf.position(0);
            readBuf.limit(readCount * Const.LONG_BYTES);
            readInBuf(minPos * Const.LONG_BYTES, readBuf, fc);
            readBuf.flip();

            while (readBuf.hasRemaining()) {
                arr[cnt++] = readBuf.getLong();
            }
            minPos += readCount;
        }
        return arr;
    }

    public IntervalSum getAvgValue(long aMin, long aMax, long tMin, long tMax, GetItem getItem) {
        IntervalSum intervalSum = new IntervalSum();
        if (tMin <= tMax && aMin <= aMax) {
            long sum = 0;
            int count = 0;
            ByteBuffer keyBuf = getItem.keyBuf;
            try {
                long minPos = lowerBound(tMin, keyBuf);
                long maxPos = upperBound(tMax, keyBuf);
                if (minPos < maxPos) {
                    ByteBuffer readBuf = getItem.buf;
                    long[] as = readLongArray(minPos, maxPos, readBuf, aFc);
                    for (int i = 0; i < as.length; i++) {
                        if (as[i] >= aMin && as[i] <= aMax) {
                            sum += as[i];
                            count++;
                        }
                    }
                    intervalSum.sum = sum;
                    intervalSum.count = count;

                    getStat(getItem, (int)(maxPos - minPos), count);
                }
            } catch (Exception e) {
                e.printStackTrace();
                print("func=getAvgValue error aMin=" + aMin + " aMax=" + aMax + " tMin" + tMin + " tMax=" + tMax);
            }
        }
        return intervalSum;
    }

    private long lowerBound(long val, ByteBuffer bb) throws IOException {
        long low = 0, high = messageCount, mid;

        bb.clear();
        while (low < high) {
            mid = low + (high - low) / 2;
            long t = readLong(mid * Const.LONG_BYTES, bb, tFc);
            if (val > t) {
                low = mid + 1;
            }
            else {
                high = mid;
            }
        }
        return low;
    }

    private long upperBound(long ele, ByteBuffer bb) throws IOException {
        long low = 0, high = messageCount, mid;

        bb.clear();
        while (low < high) {
            mid = low + (high - low) / 2;
            long t = readLong(mid * Const.LONG_BYTES, bb, tFc);
            if (t > ele) {
                high = mid;
            }
            else {
                low = mid + 1;
            }
        }
        return low;
    }

    private void readInBuf(long pos, ByteBuffer bb, FileChannel fc) {
        try {
            while (bb.hasRemaining()) {
                int read = fc.read(bb, pos);
                pos += read;
            }
        } catch (IOException e) {
            e.printStackTrace();
            //出现异常返回最大值，最终查找到的message列表就为空
            print("func=readInBuf error pos=" + pos);
        }
    }

    private long readLong(long pos, ByteBuffer bb, FileChannel fc) {
        try {
            while (bb.hasRemaining()) {
                int read = fc.read(bb, pos);
                pos += read;
            }
            bb.flip();
            long t = bb.getLong();
            bb.clear();

            return t;
        } catch (IOException e) {
            e.printStackTrace();
            //出现异常返回最大值，最终查找到的message列表就为空
            print("func=readLong error pos=" + pos);
            return Long.MAX_VALUE;
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
                print("func=putStat t is not sequence t=" + message.getT() + " prevT=" + prevMessage.getT());
                isTSequence = false;
            }
            if (!isTEqual && message.getT() == prevMessage.getT()) {
                print("func=putStat t is equal t=" + message.getT());
                isTEqual = true;
            }
            maxTInterval = Math.max(maxTInterval, message.getT() - message.getA());
        }
        prevMessage = message;
    }

    private void writeLong(FileChannel fc, ByteBuffer buf, long a) throws IOException {
        if (buf.remaining() < Const.LONG_BYTES) {
            flush(fc, buf);
        }
        buf.putLong(a);
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

                tFc.close();
                tFile.close();

                aFile.close();
                aFc.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
            print("func=close error");
        }
    }

    public void flush() {
        try {
            flush(tFc, tBuf);
            flush(aFc, aBuf);
            flush(msgFc, msgBuf);

            tBuf = null;
            aBuf = null;
            msgBuf = null;
        } catch (IOException e) {
            e.printStackTrace();
            print("func=flush error");
        }

    }
}
