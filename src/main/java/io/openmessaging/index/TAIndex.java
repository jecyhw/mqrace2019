package io.openmessaging.index;

import io.openmessaging.Const;
import io.openmessaging.codec.TDecoder;
import io.openmessaging.codec.TEncoder;
import io.openmessaging.manager.FileManager;
import io.openmessaging.model.GetAvgItem;
import io.openmessaging.model.IntervalSum;
import io.openmessaging.util.ArrayUtils;
import io.openmessaging.util.ByteBufferUtil;
import io.openmessaging.util.Utils;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by yanghuiwei on 2019-08-28
 */
public class TAIndex {

    private static final long[] tIndexArr = new long[Const.MERGE_T_INDEX_LENGTH];
    private static final int[] tMemIndexArr = new int[Const.MERGE_T_INDEX_LENGTH];
    private static int tIndexPos = 0;

    private static final ByteBuffer tBuf = ByteBuffer.allocate(Const.T_MEMORY_SIZE);
    private static final TEncoder tEncoder = new TEncoder(tBuf);

    private static final ByteBuffer aIndexArr = ByteBuffer.allocateDirect(Const.A_INDEX_LENGTH * 8);
    private static final ByteBuffer aSumArr = ByteBuffer.allocateDirect(Const.A_INDEX_LENGTH * 8);
    private static int aIndexPos = 0;


    public static void addTIndex(long chunkPrevT) {
        int index = TAIndex.tIndexPos++;

        TAIndex.tIndexArr[index] = chunkPrevT;
        TAIndex.tMemIndexArr[index] =  TAIndex.tEncoder.getBitPosition();
        TAIndex.tEncoder.resetDelta();
    }

    public static void encodeDeltaT(int deltaT) {
        tEncoder.encode(deltaT);
    }


    private static List<GetAvgItem> getItems = new ArrayList<>();

    private static ThreadLocal<GetAvgItem> getItemThreadLocal = ThreadLocal.withInitial(() -> {
        GetAvgItem getItem = new GetAvgItem();
        getItem.aIndexArr = aIndexArr.duplicate();
        getItem.aSumArr = aSumArr.duplicate();

        synchronized (TAIndex.class) {
            getItems.add(getItem);
        }
        return getItem;
    });

    public static long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        if (aMin > aMax || tMin > tMax) {
            return 0;
        }

        long startTime = System.currentTimeMillis();

        GetAvgItem getItem = getItemThreadLocal.get();
        ByteBuffer tBufDup = tBuf.duplicate();
        //对t进行精确定位，省去不必要的操作，查找的区间是左闭右开
        int beginTPos = findLeftClosedInterval(tMin, getItem.tDecoder, tBufDup);
        int endTPos = findRightOpenInterval(tMax, getItem.tDecoder, tBufDup);
        if (beginTPos >= endTPos) {
            return 0;
        }


        IntervalSum intervalSum = getItem.intervalSum;
        intervalSum.reset();

        ByteBuffer readBuf = getItem.readBuf;
        ByteBuffer aIndexBuf = getItem.aIndexArr;
        ByteBuffer aSumBuf = getItem.aSumArr;

        //处理首区间
        int beginTIndexPos = beginTPos / Const.MERGE_T_INDEX_INTERVAL;
        int firstChunkFilterReadCount = beginTPos % Const.MERGE_T_INDEX_INTERVAL;
        //处理最后尾区间，最后一个区间如果个数不等于Const.FIXED_CHUNK_SIZE，肯定会在这里处理
        int endTIndexPos = endTPos / Const.MERGE_T_INDEX_INTERVAL;
        int lastChunkNeedReadCount = endTPos % Const.MERGE_T_INDEX_INTERVAL;

        int _beginTIndexPos = beginTIndexPos;
        int readChunkAFileCount = 0, readChunkASortFileCount = 0, sumChunkASortFileCount = 0;
        int readChunkACount = 0, readChunkASortCount = 0, sumChunkASortCount = 0;

        //只有一个区间
        if (beginTIndexPos == endTIndexPos) {
            //读取按t分区的首区间剩下的a的数量
            int firstChunkNeedReadCount = lastChunkNeedReadCount - firstChunkFilterReadCount;
            FileManager.readChunkA(beginTPos, firstChunkNeedReadCount, readBuf, getItem);
            ByteBufferUtil.sumChunkA(readBuf, firstChunkNeedReadCount, aMin, aMax, intervalSum);

            getItem.map.put(firstChunkNeedReadCount, getItem.map.getOrDefault(firstChunkFilterReadCount, 0) + 1);
            readChunkAFileCount++;
            readChunkACount += firstChunkNeedReadCount;

        } else {
            long sum = 0;
            int count = 0;
            //至少两个分区，先处理首尾分区
            if (firstChunkFilterReadCount > 0) {
                int firstChunkNeedReadCount = Const.MERGE_T_INDEX_INTERVAL - firstChunkFilterReadCount;

                if (firstChunkNeedReadCount >= Const.SECOND_MERGE_T_INDEX_INTERVAL) {
                    firstChunkNeedReadCount -= Const.SECOND_MERGE_T_INDEX_INTERVAL;
                    //去二层上面读整个区间
                    SecondTAIndex.sumSecondChunkA(beginTIndexPos * 2 + 1, aMin, aMax, getItem);
                }

                if (firstChunkNeedReadCount > 0) {
                    //读取按t分区的首区间剩下的a的数量
                    FileManager.readChunkA(beginTPos, firstChunkNeedReadCount, readBuf, getItem);
                    ByteBufferUtil.sumChunkA(readBuf, firstChunkNeedReadCount, aMin, aMax, intervalSum);

                    getItem.map.put(firstChunkNeedReadCount, getItem.map.getOrDefault(firstChunkFilterReadCount, 0) + 1);
                }

                beginTIndexPos++;

                readChunkAFileCount++;
                readChunkACount += firstChunkNeedReadCount;
            }


            if (lastChunkNeedReadCount > 0) {

                if (lastChunkNeedReadCount >= Const.SECOND_MERGE_T_INDEX_INTERVAL) {
                    lastChunkNeedReadCount -= Const.SECOND_MERGE_T_INDEX_INTERVAL;
                    SecondTAIndex.sumSecondChunkA(endTIndexPos * 2, aMin, aMax, getItem);
                    if (lastChunkNeedReadCount > 0) {
                        //读取按t分区的尾区间里面的a
                        FileManager.readChunkA(endTIndexPos * Const.MERGE_T_INDEX_INTERVAL + Const.SECOND_MERGE_T_INDEX_INTERVAL, lastChunkNeedReadCount, readBuf, getItem);
                        ByteBufferUtil.sumChunkA(readBuf, lastChunkNeedReadCount, aMin, aMax, intervalSum);

                        getItem.map.put(lastChunkNeedReadCount, getItem.map.getOrDefault(firstChunkFilterReadCount, 0) + 1);
                    }
                } else {
                    //读取按t分区的尾区间里面的a
                    FileManager.readChunkA(endTIndexPos * Const.MERGE_T_INDEX_INTERVAL, lastChunkNeedReadCount, readBuf, getItem);
                    ByteBufferUtil.sumChunkA(readBuf, lastChunkNeedReadCount, aMin, aMax, intervalSum);

                    getItem.map.put(lastChunkNeedReadCount, getItem.map.getOrDefault(firstChunkFilterReadCount, 0) + 1);
                }

                readChunkAFileCount++;
                readChunkACount += lastChunkNeedReadCount;
            }

            //首尾区间处理之后，[beginTIndexPos, endTIndexPos)中的t都是符合条件，不用再判断
            while (beginTIndexPos < endTIndexPos) {

                //t区间内对a进行二分查询
                int low = beginTIndexPos * (Const.MERGE_T_INDEX_INTERVAL / Const.A_INDEX_INTERVAL),
                        high = low + (Const.MERGE_T_INDEX_INTERVAL / Const.A_INDEX_INTERVAL);
                int beginASortIndexPos = ArrayUtils.findFirstLessThanIndex(aIndexBuf, aMin, low, high);
                int endASortIndexPos = ArrayUtils.findFirstGreatThanIndex(aIndexBuf, aMax, low, high);

                //区间内没有符合条件的a
                if (beginASortIndexPos >= endASortIndexPos) {
                    beginTIndexPos++;
                    continue;
                }

                //只有一块或者首尾相连时 beginASortIndexPos=1,endASortIndexPos=2表示只有1块；beginASortIndexPos=1,endASortIndexPos=3表示只有2块，属于首尾相连
                if (beginASortIndexPos + 2 >= endASortIndexPos) {
                    //小于等于两块，一次读取
                    int chunkCount = endASortIndexPos - beginASortIndexPos;
                    int readCount = Const.A_INDEX_INTERVAL * chunkCount;
                    FileManager.readChunkASort(beginASortIndexPos * Const.A_INDEX_INTERVAL, readCount, readBuf, getItem);
                    ByteBufferUtil.sumChunkA(readBuf, readCount, aMin, aMax, intervalSum);

                    readChunkASortFileCount++;
                    readChunkASortCount += readCount;
                } else {
                    //读取第一个a区间内的的所有a
                    FileManager.readChunkASort(beginASortIndexPos * Const.A_INDEX_INTERVAL, Const.A_INDEX_INTERVAL, readBuf, getItem);
                    ByteBufferUtil.sumChunkA(readBuf, Const.A_INDEX_INTERVAL, aMin, aMax, intervalSum);
                    ++beginASortIndexPos;

                    //读取最后一个a区间内的所有a
                    endASortIndexPos--;
                    FileManager.readChunkASort(endASortIndexPos * Const.A_INDEX_INTERVAL, Const.A_INDEX_INTERVAL, readBuf, getItem);
                    ByteBufferUtil.sumChunkA(readBuf, Const.A_INDEX_INTERVAL, aMin, aMax, intervalSum);

                    readChunkASortFileCount += 2;
                    readChunkASortCount += Const.A_INDEX_INTERVAL * 2;

                    sumChunkASortFileCount += (endASortIndexPos - beginASortIndexPos);
                    sumChunkASortCount += (endASortIndexPos - beginASortIndexPos) * Const.A_INDEX_INTERVAL;
                    // 经过上面处理之后，[beginASortIndexPos, endASortIndexPos)都是符合条件的，直接累加
                    while (beginASortIndexPos < endASortIndexPos) {
                        sum += aSumBuf.getLong(beginASortIndexPos * Const.LONG_BYTES);
                        count += Const.A_INDEX_INTERVAL;
                        beginASortIndexPos++;
                    }
                }

                beginTIndexPos++;
            }
            intervalSum.add(sum, count);
        }

        getItem.costTime += (System.currentTimeMillis() - startTime);
        getItem.readHitCount += intervalSum.count;

        getItem.readChunkACount += readChunkACount;
        getItem.readChunkAFileCount += readChunkAFileCount;
        getItem.readChunkASortCount += readChunkASortCount;
        getItem.readChunkASortFileCount += readChunkASortFileCount;
        getItem.sumChunkASortCount += sumChunkASortCount;
        getItem.sumChunkASortFileCount += sumChunkASortFileCount;

        Utils.print("count:" + (endTPos - beginTPos) + ",begin:" + _beginTIndexPos + ",end:" + endTIndexPos + ",aFileCnt:" + readChunkAFileCount + ",aSortFileCnt:" + readChunkASortFileCount + ",sumASortFileCnt:" + sumChunkASortFileCount
                + ",aCnt:" + readChunkACount + ",aSortCnt:" + readChunkASortCount + ",sumASortCnt:" + sumChunkASortCount + ",cost time:" + (System.currentTimeMillis() - startTime) + ",sum:" + intervalSum.sum
                + ",count:" + intervalSum.count + ",accCostTime:" + getItem.costTime);

        return intervalSum.avg();
    }

    /**
     * 找左区间，包含[
     */
    private static int findLeftClosedInterval(long destT, TDecoder tDecoder, ByteBuffer tBufDup) {
        if (destT <= firstT) {
            return 0;
        }

        if (destT > lastT) {
            return putCount;
        }

        int beginTIndexOffset = ArrayUtils.findFirstLessThanIndex(tIndexArr, destT, 0, tIndexPos);
        long t = tIndexArr[beginTIndexOffset];
        if (t >= destT) {
            return beginTIndexOffset * Const.MERGE_T_INDEX_INTERVAL;
        }
        return tDecoder.getFirstGreatOrEqual(tBufDup, t, destT, beginTIndexOffset * Const.MERGE_T_INDEX_INTERVAL + 1, tMemIndexArr[beginTIndexOffset]);
    }

    /**
     * 找有区间，不包含)
     */
    private static int findRightOpenInterval(long destT, TDecoder tDecoder, ByteBuffer tBufDup) {
        if (destT < firstT) {
            return 0;
        }

        if (destT >= lastT) {
            return putCount;
        }
        int beginTIndexOffset = ArrayUtils.findFirstLessThanIndex(tIndexArr, destT, 0, tIndexPos);
        return findRightOpenIntervalFromMemory(beginTIndexOffset, destT, tDecoder, tBufDup);
    }

    private static int findRightOpenIntervalFromMemory(int beginTIndexOffset, long destT, TDecoder tDecoder, ByteBuffer tBufDup) {
        long t = tIndexArr[beginTIndexOffset];
        if (t > destT) {
            return beginTIndexOffset * Const.MERGE_T_INDEX_INTERVAL;
        }
        int pos = tDecoder.getFirstGreat(tBufDup, t, destT, beginTIndexOffset * Const.MERGE_T_INDEX_INTERVAL + 1, tMemIndexArr[beginTIndexOffset]);
        return pos < 0 ? findRightOpenIntervalFromMemory(beginTIndexOffset + 1, destT, tDecoder, tBufDup) : pos;
    }





    public static int putCount = 0;
    private static long firstT;
    private static long lastT;


    public static void flush(long[] t, long[] a, int len) {
        long prevT = t[0];

        if (putCount == 0) {
            firstT = prevT;
        }

        //记录块中第一个t的信息：t的值、t在内存编码中的位置
        addTIndex(prevT);
        FileManager.writeA(a[0]);

        //第一个消息单独处理，for只处理第一个消息之后的
        for (int i = 1; i < len; i++) {
            long curT = t[i];
            encodeDeltaT((int) (curT - prevT));

            prevT = curT;
            FileManager.writeA(a[i]);
        }

        completeASortAndCreateIndex(a, len);

        //更新putCount
        putCount += len;

        //更新最后一个t
        lastT = t[len - 1];
    }

    private static void completeASortAndCreateIndex(long[] a, int chunkSize) {

        for (int i = 0; i < chunkSize; i += Const.SECOND_MERGE_T_INDEX_INTERVAL) {
            SecondTAIndex.completeASortAndCreateIndex(a, i, Math.min(i + Const.SECOND_MERGE_T_INDEX_INTERVAL, chunkSize));
        }

        //按照a进行排序
        Arrays.parallelSort(a, 0, chunkSize);

        //在t的基础上在建立分区
        for (int i = 0; i < chunkSize; i += Const.A_INDEX_INTERVAL) {
            aIndexArr.putLong(aIndexPos * Const.LONG_BYTES, a[i]);
            long sumA = 0;
            int end = Math.min(i + Const.A_INDEX_INTERVAL, chunkSize);
            for (int j = i; j < end; j++) {
                FileManager.writeASort(a[j]);
                sumA += a[j];
            }
            aSumArr.putLong(aIndexPos * Const.LONG_BYTES, sumA);
            aIndexPos++;
        }
    }

    public static void flushEnd(long[] t, long[] a, int len) {
        if (len > 0) {
            flush(t, a, len);
        }
        FileManager.flushEnd();
        tEncoder.flush();
    }

    public static void log(StringBuilder sb) {
        int readChunkAFileCount = 0, readChunkASortFileCount = 0, sumChunkASortFileCount = 0;
        int readChunkACount = 0, readChunkASortCount = 0, sumChunkASortCount = 0;
        int hitCount = 0;

        sb.append("mergeCount:").append(putCount).append(",tIndexPos:").append(tIndexPos).append(",aIndexPos:").append(aIndexPos);
        sb.append(",tBytes:").append(tEncoder.getBitPosition() / 8).append(",tAllocMem:").append(tBuf.capacity());
        sb.append(",firstT:").append(firstT).append(",lastT:").append(lastT);
        sb.append("\n");

        Map<Integer, Integer> map = new HashMap<>();
        for (GetAvgItem getItem : getItems) {
            readChunkAFileCount += getItem.readChunkAFileCount;
            readChunkASortFileCount += getItem.readChunkASortFileCount;
            sumChunkASortFileCount += getItem.sumChunkASortFileCount;
            readChunkACount += getItem.readChunkACount;
            readChunkASortCount += getItem.readChunkASortCount;
            sumChunkASortCount += getItem.sumChunkASortCount;
            hitCount += getItem.readHitCount;

            getItem.map.forEach((k, v) -> map.put(k, map.getOrDefault(k, 0) + v));

            sb.append("aFileCnt:").append(getItem.readChunkAFileCount).append(",aSortFileCnt:").append(getItem.readChunkASortFileCount).append(",sumASortFileCnt:")
                    .append(getItem.sumChunkASortFileCount).append(",aCnt:").append(getItem.readChunkACount).append(",aSortCnt:").append(getItem.readChunkASortCount).append(",sumASortCnt:")
                    .append(getItem.sumChunkASortCount).append(",readFirstOrLastASortCount:")
                    .append(",hitCount:").append(getItem.readHitCount).append(",accCostTime:").append(getItem.costTime)
                    .append(",readAFileTime:").append(getItem.readAFileTime).append(",readASortFileTime:").append(getItem.readASortFileTime)
                    .append("\n");
        }

        sb.append("aFileCnt:").append(readChunkAFileCount).append(",aSortFileCnt:").append(readChunkASortFileCount).append(",sumASortFileCnt:")
                .append(sumChunkASortFileCount).append(",aCnt:").append(readChunkACount).append(",aSortCnt:").append(readChunkASortCount).append(",sumASortCnt:")
                .append(sumChunkASortCount).append(",hitCount:").append(hitCount)
                .append(",MERGE_T_INDEX_INTERVAL:").append(Const.MERGE_T_INDEX_INTERVAL).append(",MERGE_T_INDEX_LENGTH:").append(Const.MERGE_T_INDEX_LENGTH)
                .append(",FILE_NUMS:").append(Const.FILE_NUMS).append(",GET_THREAD_NUM:").append(Const.GET_THREAD_NUM)
                .append(",A_INDEX_INTERVAL:").append(Const.A_INDEX_INTERVAL).append("\n");

        map.forEach((k, v) -> sb.append("[").append(k).append(",").append(v).append("]"));
        sb.append("\n");
        SecondTAIndex.log(sb);
    }
}
