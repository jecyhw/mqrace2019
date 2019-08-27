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
    private static final AtomicInteger idAllocator = new AtomicInteger(0);
    final ByteBuffer buf = ByteBuffer.allocateDirect(Const.MEMORY_BUFFER_SIZE);

    private TEncoder tEncoder = new TEncoder(buf);
    private final long[] tArr = new long[Const.INDEX_ELE_LENGTH];
    private final int[] tOffsetArr = new int[Const.INDEX_ELE_LENGTH];
    private long firstT, lastT;

    //直接压缩到这个字节数组上
    private int msgLastBitPosition = 0;
    private final ByteBuffer msgBuf = ByteBuffer.allocate(Const.PUT_BUFFER_SIZE);
    private FileChannel msgFc;
    private final long[] msgOffsetArr = new long[Const.INDEX_ELE_LENGTH];
    private final MsgEncoder msgEncoder = new MsgEncoder(msgBuf);


    private final ByteBuffer aBuf = ByteBuffer.allocate(Const.PUT_BUFFER_SIZE);
    private FileChannel aFc;
    private final long[] aOffsetArr = new long[Const.INDEX_ELE_LENGTH];
    private int aLastBitPosition = 0;
    private ByteBuffer aCacheBlockBuf = AMemory.getCacheBuf();
    private boolean isCacheMode = true;
    private int aCacheBlockNums = 0;
    //先往缓存内存中写，写满为止
    AEncoder aEncoder = new AEncoder(aCacheBlockBuf);


    long readATime = 0;

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
        byte[] body = message.getBody();
        //比如对于1 2 3 4 5 6，间隔为2，会存 1 3 5
        if (putCount % Const.INDEX_INTERVAL == 0) {
            int blockNum = blockNums;

            //每隔INDEX_INTERVAL记录t（在indexBuf中记录了t就不会在memory中记录）
            tArr[blockNum] = t;
            //下一个t的起始位置，先写在哪个块中，再写块的便宜位置
            tOffsetArr[blockNum] = tEncoder.getBitPosition();
            tEncoder.resetDelta();

            //记录区间a的开始信息
            if (blockNum > 0) {
                //更新a的块
                updateABlock(blockNum);
                //更新body的块
                updateMsgBlock(blockNum);
            }


            aEncoder.encodeFirst(a);
            //msg不判断是由hasRemaining保证的
            msgEncoder.encodeFirst(body);

            if (putCount == 0) {
                firstT = message.getT();
            }

            blockNums++;
        } else {
            tEncoder.encode((int) (t - lastT));
            aEncoder.encode(a);
            //检查body的buf是否还有空间
            checkAndFlushMsgBuf();
            msgEncoder.encode(body);
        }

        putCount++;
        lastT = t;
    }

    private void checkAndFlushMsgBuf() {
        if (!msgEncoder.hasRemaining()) {
            msgLastBitPosition -= msgEncoder.getBitPosition();
            flush(msgFc, msgBuf);
            msgLastBitPosition += msgEncoder.getBitPosition();
        }
    }

    private void updateABlock(int blockNum) {
        int aBitPosition = aEncoder.getBitPosition();
        aOffsetArr[blockNum] = aOffsetArr[blockNum - 1] + aBitPosition - aLastBitPosition;
        aLastBitPosition = aBitPosition;

        if (isCacheMode) {
            if (!aEncoder.hasRemaining()) {
                isCacheMode = false;
                aCacheBlockBuf.putInt(aEncoder.getValue());
                aEncoder.resetBuf(aBuf);
                isCacheMode = false;
                aCacheBlockNums = blockNums;

                //重新开始
                aOffsetArr[blockNums] = 0;
                aLastBitPosition = 0;
                return;
            }
        } else {
            if (!aEncoder.hasRemaining()) {
                //落盘
                flush(aFc, aBuf);
                //aEncoder里面还有剩下需要记录下位置
                aLastBitPosition = aEncoder.getBitPosition();
            }
        }
    }


    private void updateMsgBlock(int pos) {
        int msgBitPosition = msgEncoder.getBitPosition();
        msgOffsetArr[pos] = msgOffsetArr[pos - 1] + (msgBitPosition - msgLastBitPosition);
        msgLastBitPosition = msgBitPosition;
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
            readAArray(minPos, maxPos, tLen, getItem.aDecoder, getItem.buf, as, ts);
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


    private void readMsgs(int minPos, int maxPos, GetItem getItem, long[] as, long[] ts, int len, long aMin, long aMax, long tMin, long tMax) {
        ByteBuffer readBuf = getItem.buf;
        long startOffset = msgOffsetArr[minPos];
        long endOffset = msgOffsetArr[maxPos];

        long startPos = (startOffset / 32) * 4;
        long endPos = (endOffset / 32) * 4;
        if (endOffset % 32 > 0) {
            endPos += 4;
        }
        readBuf.position(0);
        int readBytes = (int) (endPos - startPos);
        readBuf.limit(readBytes);
        //必须是一次性拿
        readInBuf(startPos, readBuf, msgFc);

        readBuf.position(0);
        //放一个4字节的哨兵
        readBuf.limit(readBytes + 4);

        MsgDecoder msgDecoder = getItem.msgDecoder;
        msgDecoder.reset(readBuf, (int) (startOffset % 32));
        List<Message> messages = getItem.messages;
        int readLen = Math.min(len, Const.INDEX_INTERVAL);
        readFirstOrLastBlockMsgs(messages, msgDecoder, as, ts, 0, readLen, aMin, aMax, tMin, tMax);

        minPos++;
        if (maxPos == minPos) {//说明读完了
            return;
        }

        maxPos--;
        while (minPos < maxPos) {
            //中间只需要判断a
            long a = as[readLen], t = ts[readLen];
            //先读区间的第一个数
            if (a >= aMin && a <= aMax) {
                byte[] body = new byte[Const.MSG_BYTES];
                msgDecoder.decodeFirst(body);
                messages.add(new Message(a, t, body));
            } else {
                msgDecoder.decodeFirstDiscard();
            }
            readLen++;

            for (int i = 1; i < Const.INDEX_INTERVAL; i++, readLen++) {
                a = as[readLen];
                t = ts[readLen];

                if (a >= aMin && a <= aMax) {
                    byte[] body = new byte[Const.MSG_BYTES];
                    msgDecoder.decode(body);
                    messages.add(new Message(a, t, body));
                } else {
                    msgDecoder.decodeDiscard();
                }
            }
            minPos++;
        }
        //读最后一块
        readFirstOrLastBlockMsgs(messages, msgDecoder, as, ts, readLen, len, aMin, aMax, tMin, tMax);
    }

    private void readFirstOrLastBlockMsgs(List<Message> messages, MsgDecoder msgDecoder, long[] as, long[] ts, int pos, int len, long aMin, long aMax, long tMin, long tMax) {
        //两头a和t都需要判断
        long a = as[pos], t = ts[pos];
        //先读区间的第一个数
        if (a >= aMin && a <= aMax && t >= tMin && t <= tMax) {
            byte[] body = new byte[Const.MSG_BYTES];
            msgDecoder.decodeFirst(body);
            messages.add(new Message(a, t, body));
        } else {
            msgDecoder.decodeFirstDiscard();
        }
        //在读区间剩下的数
        for (pos++; pos < len; pos++) {
            a = as[pos];
            t = ts[pos];

            if (a >= aMin && a <= aMax && t >= tMin && t <= tMax) {
                byte[] body = new byte[Const.MSG_BYTES];
                msgDecoder.decode(body);
                messages.add(new Message(a, t, body));
            } else {
                msgDecoder.decodeDiscard();
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

    private void readAArray(int minPos, int maxPos, int len, ADecoder aDecoder, ByteBuffer readBuf, long[] as, long[] ts) {
        if (maxPos < aCacheBlockNums) {
            //全在内存里
            readAFromMemory(minPos, maxPos, len, aDecoder, as, ts);
        } else if (minPos >= aCacheBlockNums) {
            //全在文件中
            readAFromFile(minPos, maxPos, 0, len, aDecoder, readBuf, as, ts);
        } else {
            int readLen = readAFromMemory(minPos, aCacheBlockNums, len, aDecoder, as, ts);
            if (readLen < len) {
                readAFromFile(aCacheBlockNums, maxPos, readLen, len, aDecoder, readBuf, as, ts);
            }
        }
    }

    private int readAFromMemory(int minPos, int maxPos, int len, ADecoder aDecoder, long[] as, long[] ts) {
        ByteBuffer readBuf = aCacheBlockBuf.duplicate();
        int cnt = 0;
        aDecoder.reset(readBuf, (int) aOffsetArr[minPos]);
        while (minPos < maxPos) {
            int readLen = Math.min(len - cnt, Const.INDEX_INTERVAL);
            aDecoder.decode(as, cnt, readLen);
            cnt += readLen;
            minPos++;
        }
        return cnt;
    }

    private void readAFromFile(int minPos, int maxPos, int pos, int len, ADecoder aDecoder, ByteBuffer readBuf, long[] as, long[] ts) {

        long startPos = (aOffsetArr[minPos] / 32) * 4;
        long endPos = (aOffsetArr[maxPos] / 32) * 4;
        if (aOffsetArr[maxPos] % 32 > 0) {
            endPos += 4;
        }
        readBuf.position(0);
        int readBytes = (int) (endPos - startPos);
        readBuf.limit(readBytes);
        //必须是一次性拿
        long start = System.currentTimeMillis();
        readInBuf(startPos, readBuf, aFc);
        readATime += (System.currentTimeMillis() - start);

        readBuf.position(0);
        //放一个4字节的哨兵
        readBuf.limit(readBytes + 4);

        aDecoder.reset(readBuf, (int) (aOffsetArr[minPos] % 32));

        while (minPos < maxPos) {
            int readLen = Math.min(len - pos, Const.INDEX_INTERVAL);
            aDecoder.decode(as, pos, readLen);
            pos += readLen;
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

        int minPos = fromPos / Const.INDEX_INTERVAL;
        int filterNum = fromPos % Const.INDEX_INTERVAL;
        int maxPos = endPos / Const.INDEX_INTERVAL;
        if (endPos % Const.INDEX_INTERVAL > 0) {
            maxPos++;
        }
        len += filterNum;
        readAArray(minPos, maxPos, len, getItem.aDecoder, getItem.buf, as, null);

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

        msgOffsetArr[blockNums] = msgOffsetArr[blockNums - 1] + (msgEncoder.getBitPosition() - msgLastBitPosition);
        msgEncoder.flush();
        flush(msgFc, msgBuf);

        aOffsetArr[blockNums] = aOffsetArr[blockNums - 1] + (aEncoder.getBitPosition() - aLastBitPosition);
        aEncoder.flush();
        if (isCacheMode) {
            aCacheBlockNums = blockNums;
        } else {
            flush(aFc, aBuf);
        }

        tEncoder.flushAndClear();

        try {
            Utils.print("MemoryIndex func=flush " + " blockNums:" + blockNums
                    + " putCount:" + putCount + " aFilSize:" + aFc.size() + " compressMsgFileSize:" + msgFc.size()
                    + " msgFileSize:" + ((long)putCount * Const.MSG_BYTES)
                    + " bitPos:" + tOffsetArr[blockNums - 1] / 8
                    + " bufSize:"+ buf.limit()
                    + " aCacheNums:" + aCacheBlockNums * Const.INDEX_INTERVAL
                    + " msgOffsetArr:" + msgOffsetArr[blockNums]);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
