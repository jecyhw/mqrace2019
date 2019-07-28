package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
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

    public List<Message> get(long aMin, long aMax, long tMin, long tMax) {
        List<Message> messages = new ArrayList<>();
        if (tMin <= tMax && aMin <= aMax) {
            ByteBuffer buf = ByteBuffer.allocate(Const.LONG_BYTES);
            try {
                long minPos = lowerBound(tMin, buf);
                long maxPos = upperBound(tMax, buf);

                while (minPos < maxPos) {
                    long t = readLong(minPos * Const.LONG_BYTES, buf, tFc);
                    long a = readLong(minPos * Const.LONG_BYTES, buf, aFc);
                    if (a >= aMin && a <= aMax) {
                        byte[] msg = readMsg(minPos * Const.MSG_BYTES);
                        messages.add(new Message(t, a, msg));
                    }
                    minPos++;
                }
            } catch (Exception e) {
                e.printStackTrace();
                print("func=get error aMin=" + aMin + " aMax=" + aMax + " tMin" + tMin + " tMax=" + tMax);
            }
        }
        return messages;
    }

    public IntervalSum getAvgValue(long aMin, long aMax, long tMin, long tMax) {

        IntervalSum intervalSum = new IntervalSum();
        if (tMin <= tMax && aMin <= aMax) {
            long sum = 0;
            int count = 0;
            ByteBuffer buf = ByteBuffer.allocate(Const.LONG_BYTES);
            try {
                long minPos = lowerBound(tMin, buf);
                long maxPos = upperBound(tMax, buf);

                while (minPos < maxPos) {
                    long a = readLong(minPos * Const.LONG_BYTES, buf, aFc);
                    if (a >= aMin && a <= aMax) {
                        sum += a;
                        count++;
                    }
                    minPos++;
                }
            } catch (Exception e) {
                e.printStackTrace();
                print("func=getAvgValue error aMin=" + aMin + " aMax=" + aMax + " tMin" + tMin + " tMax=" + tMax);
            }
            intervalSum.sum = sum;
            intervalSum.count = count;
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

    private byte[] readMsg(long pos) {
        ByteBuffer msgBuf = ByteBuffer.allocate(Const.MSG_BYTES);
        try {
            while (msgBuf.hasRemaining()) {
                int read = msgFc.read(msgBuf, pos);
                pos += read;
            }
            return msgBuf.array();
        } catch (Exception e) {
            e.printStackTrace();
            print("func=readMsg error pos=" + pos);
            return new byte[0];
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
        }
        prevMessage = message;
    }

    private void writeLong(FileChannel fc, ByteBuffer buf, long a) throws IOException {
        if (buf.remaining() < Const.LONG_BYTES) {
            buf.flip();
            while (buf.hasRemaining()) {
                fc.write(buf);
            }
            buf.clear();
        }
        buf.putLong(a);
    }

    private void writeMsg(byte[] msg) throws IOException {
        if (msgBuf.remaining() < msg.length) {
            msgBuf.flip();
            while (msgBuf.hasRemaining()) {
                msgFc.write(msgBuf);
            }
            msgBuf.clear();
        }
        msgBuf.put(msg);
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
}
