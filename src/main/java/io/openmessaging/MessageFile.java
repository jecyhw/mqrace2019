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

    ByteBuffer msgBuf = ByteBuffer.allocateDirect(Const.PUT_BUFFER_SIZE);
    RandomAccessFile msgFile;
    FileChannel msgFc;

    ByteBuffer aBuf = ByteBuffer.allocateDirect(Const.PUT_BUFFER_SIZE);
    RandomAccessFile aFile;
    FileChannel aFc;

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
        memoryIndex.put(message.getT(), prevMessage.getT());

        writeA(message.getA());
        writeMsg(message.getBody());
        prevMessage = message;
    }

    private void writeA(long a) {
        if (aBuf.remaining() < Const.LONG_BYTES) {
            flush(aFc, aBuf);
        }
        aBuf.putLong(a);
    }

    public List<Message> get(long aMin, long aMax, long tMin, long tMax, GetItem getItem) {
        if (tMin <= tMax && aMin <= aMax) {
            int minPos = memoryIndex.firstLessInPrimaryIndex(tMin);
            int maxPos = memoryIndex.firstGreatInPrimaryIndex(tMax);

            if (minPos >= maxPos) {
                return Collections.emptyList();
            }

            ByteBuffer readBuf = getItem.buf;

            long[] ts = getItem.ts;
            int tLen = memoryIndex.rangePosInPrimaryIndex(minPos, maxPos, ts);

            int realMinPos = minPos * Const.INDEX_INTERVAL;
            long[] as = getItem.as;
            readAArray(realMinPos, realMinPos + tLen, readBuf, as);
            //minPos，maxPos是在主内存索引的位置
            return readMsgs(realMinPos, readBuf, as, ts, tLen, aMin, aMax, tMin, tMax);
        } else {
            return Collections.emptyList();
        }
    }

    private List<Message> readMsgs(long minPos, ByteBuffer readBuf, long[] as, long[] ts, int tLen, long aMin, long aMax, long tMin, long tMax) {
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
                long a = as[s];
                if (a >= aMin && a <= aMax) {
                    byte[] body = new byte[Const.MSG_BYTES];
                    readBuf.get(body);
                    messages.add(new Message(a, ts[s], body));
                } else {
                    readBuf.position(readBuf.position() + Const.MSG_BYTES);
                }
                s++;
            }
        }
        return messages;
    }

    public void getAvgValue(long aMin, long aMax, long tMin, long tMax, IntervalSum intervalSum, GetItem getItem) {
        if (tMin <= tMax && aMin <= aMax) {
            int minPos = memoryIndex.firstLessInPrimaryIndex(tMin);
            int maxPos = memoryIndex.firstGreatInPrimaryIndex(tMax);

            long[] ts = getItem.ts;
            int tLen = memoryIndex.rangePosInPrimaryIndex(minPos, maxPos, ts);

            int realMinPos = minPos * Const.INDEX_INTERVAL;
            long[] as = getItem.as;
            readAArray(realMinPos, realMinPos + tLen, getItem.buf, as);

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
            }

            long sum = 0;
            int count = 0;
            tLen++;
            while (s < tLen) {
                long a = as[s];
                if (a >= aMin && a <= aMax) {
                    sum += a;
                    count++;
                }
                s++;
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
        flush(aFc, aBuf);
        memoryIndex.flush();
    }
}
