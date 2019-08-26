package io.openmessaging;

import org.iq80.snappy.Snappy;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.Utils.print;

/**
 * Created by yanghuiwei on 2019-07-26
 */
public class MessageFile {
    private static final AtomicInteger idAllocator = new AtomicInteger(0);
    final ByteBuffer buf = ByteBuffer.allocateDirect(Const.MEMORY_BUFFER_SIZE);

    private TEncoder tEncoder = new TEncoder(buf);
    private final long[] tArr = new long[Const.INDEX_ELE_LENGTH];
    private final int[] tOffsetArr = new int[Const.INDEX_ELE_LENGTH];
    private long firstT, lastT;

    //直接压缩到这个字节数组上
    private final byte[] msgData = new byte[Const.PUT_BUFFER_SIZE];
    private int msgDataPos = 0;
    private final ByteBuffer msgBuf = ByteBuffer.wrap(msgData);
    private RandomAccessFile msgFile;
    private long msgFileSize = 0;
    private FileChannel msgFc;
    private final byte[] uncompressMsgData = new byte[Const.COMPRESS_MSG_SIZE];
    private int uncompressMsgDataPos = 0;
    private final long[] msgOffsetArr = new long[Const.INDEX_ELE_LENGTH];

    private final ByteBuffer aBuf = ByteBuffer.allocate(Const.PUT_BUFFER_SIZE);
    private RandomAccessFile aFile;
    private FileChannel aFc;
    AEncoder aEncoder = new AEncoder(aBuf);
    private final long[] aOffsetArr = new long[Const.INDEX_ELE_LENGTH];
    private int aLastBitPosition = 0;

    //put计数
    int putCount = 0;

    //索引内存数组
    //indexBufs存的元素个数
    private int indexBufEleCount = 0;


    public MessageFile() {
        int fileId = idAllocator.getAndIncrement();
        try {
            msgFile = new RandomAccessFile(Const.STORE_PATH + fileId + Const.MSG_FILE_SUFFIX, "rw");
            msgFc = msgFile.getChannel();

            aFile = new RandomAccessFile(Const.STORE_PATH + fileId + Const.A_FILE_SUFFIX, "rw");
            aFc = aFile.getChannel();
        } catch (FileNotFoundException e) {
            print("MessageFile constructor error " + e.getMessage());
        }

    }

    public final void put(Message message) {
        if (putCount % (1024 * 1024) == 0 || putCount < 128) {
            print("func=putStat t=" + message.getT() + " " + Long.toHexString(message.getT()) + " a=" + message.getA() + " " + Long.toHexString(message.getA()) + " msg=" + Utils.bytesToHex(message.getBody()));
        }

        long t = message.getT(), a = message.getA();
        //比如对于1 2 3 4 5 6，间隔为2，会存 1 3 5
        if (putCount % Const.INDEX_INTERVAL == 0) {
            int pos = indexBufEleCount;
            //每隔INDEX_INTERVAL记录t（在indexBuf中记录了t就不会在memory中记录）
            tArr[pos] = t;
            //下一个t的起始位置，先写在哪个块中，再写块的便宜位置
            tOffsetArr[pos] = tEncoder.getBitPosition();
            tEncoder.resetDelta();

            //记录区间a的开始信息
            if (pos > 0) {
                int bitPosition = aEncoder.getBitPosition();
                aOffsetArr[pos] = aOffsetArr[pos - 1] + (bitPosition - aLastBitPosition);
                aLastBitPosition = bitPosition;
            }
            //把第一个a也编码进去，省掉一个数组
            aEncoder.encodeFirst(a);

            if (uncompressMsgDataPos == Const.COMPRESS_MSG_SIZE) {
                compressMsgBody();
            }
            //需要先压缩在赋值
            msgOffsetArr[pos] = msgFileSize + msgDataPos;

            indexBufEleCount++;

            if (putCount == 0) {
                firstT = message.getT();
            }


        } else {
            tEncoder.encode((int) (t - lastT));
            if (!aEncoder.hasRemaining()) {
                aLastBitPosition = aLastBitPosition - aEncoder.getBitPosition();
                flush(aFc, aBuf);
                aEncoder.clear();
                //aEncoder里面还有剩下的需要减掉
                aLastBitPosition += aEncoder.getBitPosition();
            }
            aEncoder.encode(a);
        }

        putCount++;

        System.arraycopy(message.getBody(), 0, uncompressMsgData, uncompressMsgDataPos, Const.MSG_BYTES);
        uncompressMsgDataPos += Const.MSG_BYTES;
        lastT = t;
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
            readAArray(minPos, maxPos, tLen, getItem.aDecoder, getItem.buf, as);
            //minPos，maxPos是在主内存索引的位置
            readMsgs(minPos, maxPos, getItem, as, ts, tLen, aMin, aMax, tMin, tMax);
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
        if (maxPos == indexBufEleCount) {
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


    private void readMsgs(int minPos, int maxPos, GetItem getItem, long[] as, long[] ts, int tLen, long aMin, long aMax, long tMin, long tMax) {
        ByteBuffer readBuf = getItem.buf;

        //计算要读取的msg
        long startOffset = msgOffsetArr[minPos];
        int len = (int)(msgOffsetArr[maxPos] - startOffset);
        readBuf.position(0);
        readBuf.limit(len);
        readInBuf(startOffset, readBuf, msgFc);

        byte[] compressMsgData = readBuf.array();
        int compressMsgDataPos = 0, compressSize;
        byte[] uncompressMsgData = getItem.uncompressMsgData;

        List<Message> messages = getItem.messages;

        int pos = 0;
        int _minPos = minPos;
        maxPos--;
        while (minPos <= maxPos) {
            compressSize = (int)(msgOffsetArr[minPos + 1] - msgOffsetArr[minPos]);
            Snappy.uncompress(compressMsgData, compressMsgDataPos, compressSize, uncompressMsgData, 0);
            compressMsgDataPos += compressSize;
            if (minPos == _minPos || minPos == maxPos) {
                for (int i = 0, uncompressMsgDataPos = 0; i < Const.INDEX_INTERVAL && pos < tLen; i++, uncompressMsgDataPos += Const.MSG_BYTES) {
                    long a = as[pos], t = ts[pos];

                    if (a >= aMin && a <= aMax && t >= tMin && t <= tMax) {
                        getMessage(uncompressMsgData, messages, uncompressMsgDataPos, a, t);
                    }
                    pos++;
                }
            } else {
                for (int i = 0, uncompressMsgDataPos = 0; i < Const.INDEX_INTERVAL && pos < tLen; i++, uncompressMsgDataPos += Const.MSG_BYTES) {
                    long a = as[pos];
                    if (a >= aMin && a <= aMax) {
                        getMessage(uncompressMsgData, messages, uncompressMsgDataPos, a, ts[pos]);
                    }
                    pos++;
                }
            }


            minPos++;
        }
    }

    private void getMessage(byte[] uncompressMsgData, List<Message> messages, int uncompressMsgDataPos, long a, long t) {
        byte[] body = new byte[Const.MSG_BYTES];
        System.arraycopy(uncompressMsgData, uncompressMsgDataPos, body, 0, Const.MSG_BYTES);
        messages.add(new Message(a, t, body));
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

    private void readAArray(int minPos, int maxPos, int len, ADecoder aDecoder, ByteBuffer readBuf, long[] as) {
        long startPos = (aOffsetArr[minPos] / 32) * 4;
        long endPos = (aOffsetArr[maxPos] / 32) * 4;
        if (aOffsetArr[maxPos] % 32 > 0) {
            endPos += 4;
        }
        readBuf.position(0);
        int readBytes = (int) (endPos - startPos);
        readBuf.limit(readBytes);
        //必须是一次性拿
        readInBuf(startPos, readBuf, aFc);
        readBuf.position(0);
        //放一个4字节的哨兵
        readBuf.limit(readBytes + 4);

        int cnt = 0;
        aDecoder.reset(readBuf, (int) (aOffsetArr[minPos] % 32));

        while (minPos < maxPos) {
            int readLen = Math.min(len - cnt, Const.INDEX_INTERVAL);
            aDecoder.decode(as, cnt, readLen);
            cnt += readLen;
            minPos++;
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

    private void sumAInRangeT(int fromPos, int endPos, long aMin, long aMax, long tMin, long tMax, IntervalSum intervalSum, GetItem getItem) {
        int len = endPos - fromPos;
        long[] as = getItem.as;

        int minPos = fromPos / Const.INDEX_INTERVAL;
        int filterNum = fromPos % Const.INDEX_INTERVAL;
        int maxPos = endPos / Const.INDEX_INTERVAL;
        if (endPos % Const.INDEX_INTERVAL > 0) {
            maxPos++;
        }
        len += filterNum;
        readAArray(minPos, maxPos, len, getItem.aDecoder, getItem.buf, as);

        long sum = 0;
        int count = 0;
        for (int i = filterNum; i < len; i++) {
            long a = as[i];
            if (a >= aMin && a <= aMax) {
                sum += a;
                count++;
            }
        }
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
    private int firstLessInPrimaryIndex(long val) {
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

    public final void flush() {
        //最后一块进行压缩
        msgDataPos += Snappy.compress(uncompressMsgData, 0, Const.COMPRESS_MSG_SIZE, msgData, msgDataPos);
        flushMsg();
        msgOffsetArr[indexBufEleCount] = msgFileSize + msgDataPos;

        aOffsetArr[indexBufEleCount] = aOffsetArr[indexBufEleCount - 1] + (aEncoder.getBitPosition() - aLastBitPosition);
        aEncoder.flush();
        flush(aFc, aBuf);


        tEncoder.flushAndClear();


        try {
            Utils.print("MemoryIndex func=flush " + " indexBufEleCount:" + indexBufEleCount
                    + " putCount:" + putCount + " aFilSize:" + aFile.length() + " compressMsgFileSize:" + msgFile.length()
                    + " msgFileSize:" + ((long)putCount * Const.MSG_BYTES)
                    + " bitPos:" + tOffsetArr[indexBufEleCount - 1] / 8
                    + " bufSize:"+ buf.limit()
                    + " msgOffsetArr:" + msgOffsetArr[indexBufEleCount]);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
