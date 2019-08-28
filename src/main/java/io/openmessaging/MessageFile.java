package io.openmessaging;

import io.openmessaging.codec.*;
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
    final ByteBuffer buf = ByteBuffer.allocate(Const.PUT_BUFFER_SIZE * 2);

    private static final AtomicInteger idAllocator = new AtomicInteger(0);
    final ByteBuffer tBuf = ByteBuffer.allocateDirect(Const.MEMORY_BUFFER_SIZE);
    private TEncoder tEncoder = new TEncoder(tBuf);
    private final long[] tArr = new long[Const.INDEX_ELE_LENGTH];
    private final int[] tOffsetArr = new int[Const.INDEX_ELE_LENGTH];
    private long firstT, lastT;

    //直接压缩到这个字节数组上
    private final ByteBuffer msgBuf;
    private FileChannel msgFc;

    private final ByteBuffer aBuf;
    private FileChannel aFc;
    private ByteBuffer aCacheBlockBuf = AMemory.getCacheBuf();
    private boolean isCacheMode = true;
    private int aCacheNums = 0;

    //put计数
    private int putCount = 0;

    //索引内存数组
    //indexBufs存的元素个数
    private int blockNums = 0;


    public MessageFile() {
        buf.limit(Const.PUT_BUFFER_SIZE);
        msgBuf = buf.slice();

        buf.position(Const.PUT_BUFFER_SIZE);
        buf.limit(Const.PUT_BUFFER_SIZE * 2);
        aBuf = buf.slice();


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

            if (putCount == 0) {
                firstT = message.getT();
            }

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
        if (isCacheMode) {
            if (!aCacheBlockBuf.hasRemaining()) {
                isCacheMode = false;
                aBuf.putLong(a);
            } else {
                aCacheBlockBuf.putLong(a);
            }
        } else {
            if (!aBuf.hasRemaining()) {
                flush(aFc, aBuf);
            }
            aBuf.putLong(a);
        }
    }

    public final void get(long aMin, long aMax, long tMin, long tMax, GetItem getItem, ByteBuffer tBuf) {
        if (tMin <= tMax && aMin <= aMax) {
            int minPos = firstLessInPrimaryIndex(tMin);
            int maxPos = firstGreatInPrimaryIndex(tMax);

            if (minPos >= maxPos) {
                return;
            }

            long[] ts = getItem.ts;
            int tLen = rangePosInPrimaryIndex(minPos, maxPos, ts, getItem, tBuf);

            long[] as = getItem.as;
            readAArray(minPos * Const.INDEX_INTERVAL, maxPos * Const.INDEX_INTERVAL, tLen, getItem.buf, as);
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
    private int rangePosInPrimaryIndex(int minPos, int maxPos, long[] destT, GetItem getItem, ByteBuffer tBuf) {
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

    private void readMsgs(long fileStartPos, GetItem getItem, long[] as, long[] ts, int len, long aMin, long aMax, long tMin, long tMax) {
        ByteBuffer readBuf = getItem.buf;

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


    public final void getAvgValue(long aMin, long aMax, long tMin, long tMax, IntervalSum intervalSum, GetItem getItem, ByteBuffer tBuf) {
        if (tMin <= tMax && aMin <= aMax) {
            int fromPos = findLeftClosedInterval(tMin, getItem, tBuf);
            int endPos = findRightOpenInterval(tMax, getItem, tBuf);
            if (fromPos >= endPos) {
                return;
            }

            sumAInRangeT(fromPos, endPos, aMin, aMax, tMin, tMax, intervalSum, getItem);
        }
    }

    private void readAArray(int fileStartPos, int fileEndPos, int len, ByteBuffer readBuf, long[] as) {
        if (fileEndPos < aCacheNums) {
            //全在内存里
            readAFromMemory(fileStartPos * Const.LONG_BYTES, len, as);
        } else if (fileStartPos >= aCacheNums) {
            //全在文件中
            readAFromFile((fileStartPos - aCacheNums) * Const.LONG_BYTES, len, readBuf, as, 0);
        } else {
            int readLen = Math.min(len, aCacheNums - fileStartPos);
            readAFromMemory(fileStartPos * Const.LONG_BYTES, readLen, as);
            if (readLen < len) {
                readAFromFile(0, len - readLen, readBuf, as, readLen);
            }
        }
    }

    private void readAFromMemory(int pos, int len, long[] as) {
        ByteBuffer readBuf = aCacheBlockBuf.duplicate();
        readBuf.position(pos);
        for (int cnt = 0; cnt < len; cnt++) {
            as[cnt] = readBuf.getLong();
        }
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
            Utils.print("func=flush error");
        }
    }

    private void sumAInRangeT(int fromPos, int endPos, long aMin, long aMax, long tMin, long tMax, IntervalSum intervalSum, GetItem getItem) {
        int len = endPos - fromPos;
        long[] as = getItem.as;

        readAArray(fromPos, endPos, len, getItem.buf, as);

        StringBuilder sb = new StringBuilder();
        sb.append("[s:").append(fromPos / Const.INDEX_INTERVAL).append("][e:").append(endPos / Const.INDEX_INTERVAL);
        int begin = fromPos % Const.INDEX_INTERVAL;
        sb.append("][sL:").append(begin).append("][sE:").append(endPos % Const.INDEX_INTERVAL).append("]");
        int edge = 0;
        if (begin > 0) {
            edge += Const.INDEX_INTERVAL - begin;
        }
        edge += endPos % Const.INDEX_INTERVAL;
        sb.append("[edge:").append(edge).append("]");

        int max = 0; int min = 0;

        long sum = 0;
        int count = 0;
        for (int i = 0; i < len; i++) {
            long a = as[i];
            if (a >= aMin && a <= aMax) {
                sum += a;
                count++;
            } else {
                if (a > aMax) {
                    max++;
                } else {
                    min++;
                }
            }
        }
        sb.append("[max:").append(max).append("][min:").append(min).append("]").append("[cnt:").append(count).append("]").append("\n");
        Utils.print(sb.toString());

        intervalSum.add(sum, count);
    }

    /**
     * 找左区间，包含[
     */
    private int findLeftClosedInterval(long destT, GetItem getItem, ByteBuffer tBuf) {
        if (destT <= firstT) {
            return 0;
        }

        if (destT > lastT) {
            return putCount;
        }

        int minPos = firstLessInPrimaryIndex(destT);
        long t = tArr[minPos];
        if (t >= destT) {
            return minPos * Const.INDEX_INTERVAL;
        }
        return getItem.tDecoder.getFirstGreatOrEqual(tBuf, t, destT, minPos * Const.INDEX_INTERVAL + 1, tOffsetArr[minPos]);
    }

    private int findRightOpenInterval(long destT, GetItem getItem, ByteBuffer tBuf) {
        if (destT < firstT) {
            return 0;
        }

        if (destT >= lastT) {
            return putCount;
        }
        int minPos = firstLessInPrimaryIndex(destT);
        return findRightOpenIntervalFromMemory(minPos, destT, getItem, tBuf);
    }

    private int findRightOpenIntervalFromMemory( int minPos, long destT, GetItem getItem, ByteBuffer tBuf) {
        long t = tArr[minPos];
        if (t > destT) {
            return minPos * Const.INDEX_INTERVAL;
        }
        int pos = getItem.tDecoder.getFirstGreat(tBuf, t, destT, minPos * Const.INDEX_INTERVAL + 1, tOffsetArr[minPos]);
        return pos < 0 ? findRightOpenIntervalFromMemory(minPos + 1, destT, getItem, tBuf) : pos;
    }

    private int firstGreatInPrimaryIndex(long val) {
        int low = 0, high = blockNums, mid;
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
    private int firstLessInPrimaryIndex(long val) {
        int low = 0, high = blockNums, mid;

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

    public final void flush() {
        //最后一块进行压缩
        flush(msgFc, msgBuf);

        //最后计算
        aCacheNums = aCacheBlockBuf.position() / Const.LONG_BYTES;

        flush(aFc, aBuf);
        tEncoder.flushAndClear();

        try {
            Utils.print("MemoryIndex func=flush " + " blockNums:" + blockNums
                    + " putCount:" + putCount + " aFilSize:" + aFc.size() + " compressMsgFileSize:" + msgFc.size()
                    + " msgFileSize:" + ((long)putCount * Const.MSG_BYTES)
                    + " bitPos:" + tOffsetArr[blockNums - 1] / 8
                    + " bufSize:"+ tBuf.limit()
                    + " aCacheNums:" + aCacheNums);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
