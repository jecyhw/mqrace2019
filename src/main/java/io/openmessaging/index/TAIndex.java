package io.openmessaging.index;

import io.openmessaging.Const;
import io.openmessaging.codec.TDecoder;
import io.openmessaging.codec.TEncoder;
import io.openmessaging.manager.FileManager;
import io.openmessaging.model.GetAvgItem;
import io.openmessaging.model.IntervalSum;
import io.openmessaging.model.Ta;
import io.openmessaging.util.ArrayUtils;
import io.openmessaging.util.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by yanghuiwei on 2019-08-28
 */
public class TAIndex {

    private static ExecutorService executorService = Executors.newFixedThreadPool(Const.GET_THREAD_NUM);

    private static long[] tIndexArr = new long[Const.MERGE_T_INDEX_LENGTH];
    private static int[] tMemIndexArr = new int[Const.MERGE_T_INDEX_LENGTH];
    private static int tIndexPos = 0;

    private static final ByteBuffer tBuf = ByteBuffer.allocate(Const.T_MEMORY_SIZE);
    private static TEncoder tEncoder = new TEncoder(tBuf);

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

        long[] as = getItem.as;
        ByteBuffer readBuf = getItem.readBuf;
        ByteBuffer aIndexBuf = getItem.aIndexArr;
        ByteBuffer aSumBuf = getItem.aSumArr;

        //处理首区间
        int beginTIndexPos = beginTPos / Const.MERGE_T_INDEX_INTERVAL;
        //处理最后尾区间，最后一个区间如果个数不等于Const.FIXED_CHUNK_SIZE，肯定会在这里处理
        int firstChunkFilterReadCount = beginTPos % Const.MERGE_T_INDEX_INTERVAL;

        int endTIndexPos = endTPos / Const.MERGE_T_INDEX_INTERVAL;
        int lastChunkNeedReadCount = endTPos % Const.MERGE_T_INDEX_INTERVAL;

        int _beginTIndexPos = beginTIndexPos;
        int readChunkAFileCount = 0, readChunkASortFileCount = 0, sumChunkASortFileCount = 0;
        int readChunkACount = 0, readChunkASortCount = 0, sumChunkASortCount = 0;

        //只有一个区间
        if (beginTIndexPos == endTIndexPos) {
            //读取按t分区的首区间剩下的a的数量
            int firstChunkNeedReadCount = lastChunkNeedReadCount - firstChunkFilterReadCount;
            FileManager.readChunkA(beginTPos, as, firstChunkNeedReadCount, readBuf, getItem);
            sumChunkA(as, firstChunkNeedReadCount, aMin, aMax, intervalSum);

            readChunkAFileCount++;
            readChunkACount += firstChunkNeedReadCount;

        } else {
            long sum = 0;
            int count = 0;
            int sumTaskCount = 0;

            ExecutorCompletionService<IntervalSum> sumExecutorCompletionService = new ExecutorCompletionService<>(executorService);
            //至少两个分区，先处理首尾分区
            if (firstChunkFilterReadCount > 0) {
                //读取按t分区的首区间剩下的a的数量
                int firstChunkNeedReadCount = Const.MERGE_T_INDEX_INTERVAL - firstChunkFilterReadCount;
                subSumTask(sumExecutorCompletionService, beginTPos, firstChunkNeedReadCount, aMin, aMax);
                sumTaskCount++;

                beginTIndexPos++;

                readChunkAFileCount++;
                readChunkACount += firstChunkNeedReadCount;
            }

            if (lastChunkNeedReadCount > 0) {
                //读取按t分区的尾区间里面的a
                subSumTask(sumExecutorCompletionService, endTIndexPos * Const.MERGE_T_INDEX_INTERVAL, lastChunkNeedReadCount, aMin, aMax);
                sumTaskCount++;

                readChunkAFileCount++;
                readChunkACount += lastChunkNeedReadCount;
            }

            //首尾区间处理之后，[beginTIndexPos, endTIndexPos)中的t都是符合条件，不用再判断
            while (beginTIndexPos < endTIndexPos) {

                //t区间内对a进行二分查询
                int low = beginTIndexPos * (Const.MERGE_T_INDEX_INTERVAL / Const.A_INDEX_INTERVAL), high = low + (Const.MERGE_T_INDEX_INTERVAL / Const.A_INDEX_INTERVAL);
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

                    subSumTask(sumExecutorCompletionService, beginASortIndexPos * Const.A_INDEX_INTERVAL, readCount, aMin, aMax);
                    sumTaskCount++;

                    readChunkASortFileCount++;
                    readChunkASortCount += readCount;
                } else {
                    //读取第一个a区间内的的所有a
                    subSumTask(sumExecutorCompletionService, beginASortIndexPos * Const.A_INDEX_INTERVAL, Const.A_INDEX_INTERVAL, aMin, aMax);
                    sumTaskCount++;

                    ++beginASortIndexPos;

                    //读取最后一个a区间内的所有a
                    endASortIndexPos--;
                    subSumTask(sumExecutorCompletionService, endASortIndexPos * Const.A_INDEX_INTERVAL, Const.A_INDEX_INTERVAL, aMin, aMax);
                    sumTaskCount++;

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

            addSumSum(sumExecutorCompletionService, intervalSum, sumTaskCount);
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

    private static void addSumSum(ExecutorCompletionService<IntervalSum> sumExecutorCompletionService, IntervalSum intervalSum, int sumTaskCount) {
        try {
            while (sumTaskCount > 0) {
                IntervalSum subSum = sumExecutorCompletionService.take().get();
                intervalSum.add(subSum.sum, subSum.count);
                sumTaskCount--;
            }
        } catch (InterruptedException | ExecutionException e) {
            Utils.print("");
        }
    }

    private static void subSumTask(ExecutorCompletionService<IntervalSum> sumExecutorCompletionService, int beginTPos, int firstChunkNeedReadCount, long aMin, long aMax) {
        sumExecutorCompletionService.submit(() -> {
            GetAvgItem subGetAvgItem =  getItemThreadLocal.get();
            long[] subAs = subGetAvgItem.as;
            FileManager.readChunkA(beginTPos, subAs, firstChunkNeedReadCount, subGetAvgItem.readBuf, subGetAvgItem);
            IntervalSum subSum = new IntervalSum();
            sumChunkA(subAs, firstChunkNeedReadCount, aMin, aMax, subSum);
            return subSum;
        });
    }

    private static void sumChunkA(long[] as, int len, long aMin, long aMax, IntervalSum intervalSum) {
        long sum = 0;
        int count = 0;
        for (int i = 0; i < len; i++) {
            long a = as[i];
            if (aMin <= a && a <= aMax) {
                count++;
                sum += a;
            }
        }
        intervalSum.add(sum, count);
    }

    /**
     * @param indexPos
     * @param tDecoder
     * @param ts
     * @param tBufDup
     * @return 返回读取的条数
     */
    private static int readChunkT(int indexPos, long[] ts, TDecoder tDecoder, ByteBuffer tBufDup) {
        ts[0] = tIndexArr[indexPos];
        if (indexPos == tIndexPos - 1) {
            //最后一个chunk需要根据putCount来计算
            int readLen = (putCount - 1) % Const.MERGE_T_INDEX_INTERVAL;
            tDecoder.decode(tBufDup, ts, 1, tMemIndexArr[indexPos], readLen);
            return readLen + 1;
        } else {
            tDecoder.decode(tBufDup, ts, 1, tMemIndexArr[indexPos], Const.MERGE_T_INDEX_INTERVAL - 1);
            return Const.MERGE_T_INDEX_INTERVAL;
        }
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


    public static void flush(Ta[] ta , int len) {
        long prevT = ta[0].t;

        if (putCount == 0) {
            firstT = prevT;
        }

        //记录块中第一个t的信息：t的值、t在内存编码中的位置
        addTIndex(prevT);
        FileManager.writeA(ta[0].a);

        //第一个消息单独处理，for只处理第一个消息之后的
        for (int i = 1; i < len; i++) {
            long curT = ta[i].t;
            encodeDeltaT((int) (curT - prevT));

            prevT = curT;
            FileManager.writeA(ta[i].a);
        }

        completeASortAndCreateIndex(ta, len);

        //更新putCount
        putCount += len;

        //更新最后一个t
        lastT = ta[len - 1].t;
    }

    private static Comparator<Ta> taComparator = (o1, o2) -> Long.compare(o1.a, o2.a);
    private static void completeASortAndCreateIndex(Ta[] ta, int chunkSize) {
        //按照a进行排序
        Arrays.parallelSort(ta, 0, chunkSize, taComparator);


        //在t的基础上在建立分区
        for (int i = 0; i < chunkSize; i += Const.A_INDEX_INTERVAL) {
            aIndexArr.putLong(aIndexPos * Const.LONG_BYTES, ta[i].a);
            long sumA = 0;
            int end = Math.min(i + Const.A_INDEX_INTERVAL, chunkSize);
            for (int j = i; j < end; j++) {
                FileManager.writeASort(ta[j].a);
                sumA += ta[j].a;
            }
            aSumArr.putLong(aIndexPos * Const.LONG_BYTES, sumA);
            aIndexPos++;
        }
        // TODO 需要验证
//        aIndexArr[aIndexPos] = ta[chunkSize - 1].a;
    }

    public static void flushEnd(Ta[] ta, int len) {
        if (len > 0) {
            flush(ta, len);
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

        for (GetAvgItem getItem : getItems) {
            readChunkAFileCount += getItem.readChunkAFileCount;
            readChunkASortFileCount += getItem.readChunkASortFileCount;
            sumChunkASortFileCount += getItem.sumChunkASortFileCount;
            readChunkACount += getItem.readChunkACount;
            readChunkASortCount += getItem.readChunkASortCount;
            sumChunkASortCount += getItem.sumChunkASortCount;
            hitCount += getItem.readHitCount;

            sb.append("aFileCnt:").append(getItem.readChunkAFileCount).append(",aSortFileCnt:").append(getItem.readChunkASortFileCount).append(",sumASortFileCnt:")
                    .append(getItem.sumChunkASortFileCount).append(",aCnt:").append(getItem.readChunkACount).append(",aSortCnt:").append(getItem.readChunkASortCount).append(",sumASortCnt:")
                    .append(getItem.sumChunkASortCount).append(",readFirstOrLastASortCount:")
                    .append(",hitCount:").append(getItem.readHitCount).append(",accCostTime:").append(getItem.costTime)
                    .append(",readAFileTime:").append(getItem.readAFileTime).append(",readASortFileTime:").append(getItem.readASortFileTime).append("\n");
        }

        sb.append("aFileCnt:").append(readChunkAFileCount).append(",aSortFileCnt:").append(readChunkASortFileCount).append(",sumASortFileCnt:")
                .append(sumChunkASortFileCount).append(",aCnt:").append(readChunkACount).append(",aSortCnt:").append(readChunkASortCount).append(",sumASortCnt:")
                .append(sumChunkASortCount).append(",hitCount:").append(hitCount)
                .append(",MERGE_T_INDEX_INTERVAL:").append(Const.MERGE_T_INDEX_INTERVAL).append(",MERGE_T_INDEX_LENGTH:").append(Const.MERGE_T_INDEX_LENGTH)
                .append(",FILE_NUMS:").append(Const.FILE_NUMS).append("\n");
    }
}
