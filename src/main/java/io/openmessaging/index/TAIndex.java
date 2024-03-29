package io.openmessaging.index;

import io.netty.util.concurrent.FastThreadLocal;
import io.openmessaging.Const;
import io.openmessaging.codec.TDecoder;
import io.openmessaging.codec.TEncoder;
import io.openmessaging.model.GetAvgItem;
import io.openmessaging.model.IntervalSum;
import io.openmessaging.partition.PartitionIndex;
import io.openmessaging.partition.SinglePartitionFile;
import io.openmessaging.util.ArrayUtils;
import io.openmessaging.util.ByteBufferUtil;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yanghuiwei on 2019-08-28
 */
public class TAIndex {
    public static TAIndex taIndex = new TAIndex();

    private final long[] tIndexArr = new long[Const.MAX_T_INDEX_LENGTH];
    private final int[] tMemIndexArr = new int[Const.MAX_T_INDEX_LENGTH];
    private int tIndexPos = 0;


    private final ByteBuffer tBuf = ByteBuffer.allocateDirect(Const.T_MEMORY_SIZE);
    private final TEncoder tEncoder = new TEncoder(tBuf);

    private PartitionIndex[] partitionIndices = new PartitionIndex[Const.T_INDEX_INTERVALS.length];
    private SinglePartitionFile aFile;

    public TAIndex() {
        createPartitionIndex();
        aFile = new SinglePartitionFile(Const.MAX_T_INDEX_INTERVAL);
    }

    private void createPartitionIndex() {
        for (int i = 0; i < Const.T_INDEX_INTERVALS.length; i++) {
            partitionIndices[i] = new PartitionIndex(Const.T_INDEX_INTERVALS[i]);
        }
    }

    public void addTIndex(long chunkPrevT) {
        int index = tIndexPos++;

        tIndexArr[index] = chunkPrevT;
        tMemIndexArr[index] =  tEncoder.getBitPosition();
        tEncoder.resetDelta();
    }

    public void encodeDeltaT(int deltaT) {
        tEncoder.encode(deltaT);
    }


    private static List<GetAvgItem> getItems = new ArrayList<>();

    private static FastThreadLocal<GetAvgItem> getItemThreadLocal = new FastThreadLocal<GetAvgItem>() {
        @Override
        public GetAvgItem initialValue() {
            GetAvgItem getItem = new GetAvgItem();
            synchronized (TAIndex.class) {
                getItems.add(getItem);
            }
            return getItem;
        }
    };

    private AtomicInteger onceCounter = new AtomicInteger(0);

    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        if (aMin > aMax || tMin > tMax) {
            return 0;
        }

        GetAvgItem getItem = getItemThreadLocal.get();
        ByteBuffer tBufDup = tBuf.duplicate();
        //对t进行精确定位，省去不必要的操作，查找的区间是左闭右开
        int beginTPos = findLeftClosedInterval(tMin, getItem.tDecoder, tBufDup);
        int endTPos = findRightOpenInterval(tMax, getItem.tDecoder, tBufDup);
        if (beginTPos >= endTPos) {
            return 0;
        }

        //t符合提交的个数
        int tCount = endTPos - beginTPos;
        IntervalSum intervalSum = getItem.intervalSum;
        intervalSum.reset();

        if (tCount <= Const.T_INDEX_INTERVALS[Const.T_INDEX_INTERVALS.length - 1]) {
            //读取的数量比最小层的间隔还小，直接从排序后的t对应的a文件读取过滤a求平均值返回
            readAndSumFromAPartition(beginTPos, tCount, aMin, aMax, getItem);
        } else {
            //走分层索引查询
            sumByPartitionIndex(beginTPos, endTPos, tCount, aMin, aMax, getItem);
        }
        return intervalSum.avg();
    }

    private void sumByPartitionIndex(int beginTPos, int endTPos, int tCount, long aMin, long aMax, GetAvgItem getItem) {
        PartitionIndex partitionIndex = findBestPartitionIndex(beginTPos, endTPos);
        if (partitionIndex == null) { //没有找到最合适的索引，直接从排序后的t对应的a文件读取过滤a求平均值返回
            readAndSumFromAPartition(beginTPos, tCount, aMin, aMax, getItem);
            return;
        }

        int interval = partitionIndex.getInterval();
        int doubleHalfInterval = interval / 4; //4分1的分区大小
        int beginPartition = beginTPos / interval, endPartition = endTPos / interval;//求首尾所在分区
        int firstPartitionFilterCount = beginTPos % interval, lastPartitionNeedCount = endTPos % interval;//求首分区需要过滤的个数，尾分区需要读取的个数
        long sum = 0;
        int count = 0;

        if (firstPartitionFilterCount > 0) {//处理首分区
            int firstReadCount = interval - firstPartitionFilterCount;
            if (firstPartitionFilterCount < doubleHalfInterval) {
                //求反，先减后加，防止溢出
                inverseReadAndSumFromAPartition(beginTPos - firstPartitionFilterCount, firstPartitionFilterCount, aMin, aMax, getItem);
                partitionIndex.partitionSum(beginPartition, aMin, aMax, getItem);
            } else {
                sumByPartitionIndex(beginTPos, beginTPos + firstReadCount, firstReadCount, aMin, aMax, getItem);
            }
            beginPartition++;
        }

        if (lastPartitionNeedCount > 0) {//处理尾分区
            if (interval - lastPartitionNeedCount < doubleHalfInterval && (endTPos - endPartition * interval) >= interval) {
                //求反，先减后加，防止溢出
                inverseReadAndSumFromAPartition(endTPos, interval - lastPartitionNeedCount, aMin, aMax, getItem);
                partitionIndex.partitionSum(endPartition, aMin, aMax, getItem);
            } else {
                sumByPartitionIndex(endTPos - lastPartitionNeedCount, endTPos, lastPartitionNeedCount, aMin, aMax, getItem);
            }
        }

        //首尾区间处理之后，[beginPartition, endPartition)中的t都是符合条件，不用再判断
        while (beginPartition < endPartition) {
            partitionIndex.partitionSum(beginPartition, aMin, aMax, getItem);
            beginPartition++;
        }
        getItem.intervalSum.add(sum, count);
    }

    private PartitionIndex findBestPartitionIndex(int beginTPos, int endTPos) {
        for (int i = 0; i < Const.T_INDEX_INTERVALS.length; i++){
            int interval = partitionIndices[i].getInterval();
            int beginPartition = beginTPos / interval;
            int endPartition = endTPos / interval;

            if (endPartition - beginPartition > 1) {//找到了最合适的分区
                return partitionIndices[i];
            }
        }
        return null;
    }


    private void readAndSumFromAPartition(int offsetCount, int readCount, long aMin, long aMax, GetAvgItem getItem) {
        ByteBuffer readBuf = getItem.readBuf;
        //读取按t分区的首区间剩下的a的数量
        aFile.readPartition(offsetCount, readCount, readBuf, getItem);
        ByteBufferUtil.sumChunkA(readBuf, readCount, aMin, aMax, getItem.intervalSum);

//        getItem.map.put(readCount, getItem.map.getOrDefault(readCount, 0) + 1);
    }

    private void inverseReadAndSumFromAPartition(int offsetCount, int readCount, long aMin, long aMax, GetAvgItem getItem) {
        ByteBuffer readBuf = getItem.readBuf;
        //读取按t分区的首区间剩下的a的数量
        aFile.readPartition(offsetCount, readCount, readBuf, getItem);
        ByteBufferUtil.inverseSumChunkA(readBuf, readCount, aMin, aMax, getItem.intervalSum);

//        getItem.map.put(readCount, getItem.map.getOrDefault(readCount, 0) + 1);
    }

    /**
     * 找左区间，包含[
     */
    private int findLeftClosedInterval(long destT, TDecoder tDecoder, ByteBuffer tBufDup) {
        if (destT <= firstT) {
            return 0;
        }

        if (destT > lastT) {
            return putCount;
        }

        int beginTIndexOffset = ArrayUtils.findFirstLessThanIndex(tIndexArr, destT, 0, tIndexPos);
        long t = tIndexArr[beginTIndexOffset];
        if (t >= destT) {
            return beginTIndexOffset * Const.MAX_T_INDEX_INTERVAL;
        }
        return tDecoder.getFirstGreatOrEqual(tBufDup, t, destT, beginTIndexOffset * Const.MAX_T_INDEX_INTERVAL + 1, tMemIndexArr[beginTIndexOffset]);
    }

    /**
     * 找有区间，不包含)
     */
    private int findRightOpenInterval(long destT, TDecoder tDecoder, ByteBuffer tBufDup) {
        if (destT < firstT) {
            return 0;
        }

        if (destT >= lastT) {
            return putCount;
        }
        int beginTIndexOffset = ArrayUtils.findFirstLessThanIndex(tIndexArr, destT, 0, tIndexPos);
        return findRightOpenIntervalFromMemory(beginTIndexOffset, destT, tDecoder, tBufDup);
    }

    private int findRightOpenIntervalFromMemory(int beginTIndexOffset, long destT, TDecoder tDecoder, ByteBuffer tBufDup) {
        long t = tIndexArr[beginTIndexOffset];
        if (t > destT) {
            return beginTIndexOffset * Const.MAX_T_INDEX_INTERVAL;
        }
        int pos = tDecoder.getFirstGreat(tBufDup, t, destT, beginTIndexOffset * Const.MAX_T_INDEX_INTERVAL + 1, tMemIndexArr[beginTIndexOffset]);
        return pos < 0 ? findRightOpenIntervalFromMemory(beginTIndexOffset + 1, destT, tDecoder, tBufDup) : pos;
    }



    public int putCount = 0;
    private long firstT;
    private long lastT;


    public void createIndex(long[] t, long[] a, int len) {
        long prevT = t[0];

        if (putCount == 0) {
            firstT = prevT;
        }

        //记录块中第一个t的信息：t的值、t在内存编码中的位置
        addTIndex(prevT);
        aFile.writeA(a[0]);

        //第一个消息单独处理，for只处理第一个消息之后的
        for (int i = 1; i < len; i++) {
            long curT = t[i];
            encodeDeltaT((int) (curT - prevT));

            prevT = curT;
            aFile.writeA(a[i]);
        }

        //更新putCount
        putCount += len;

        //更新最后一个t
        lastT = t[len - 1];
    }



    public void flush(long[] t, long[] a, int len) {
        if (len > 0) {
            createIndex(t, a, len);
        }

        aFile.flush();
        tEncoder.flush();
        for (PartitionIndex partitionIndex : partitionIndices) {
            partitionIndex.flush();
        }
    }

    public void putA(long a) {
        for (PartitionIndex partitionIndex : partitionIndices) {
            partitionIndex.putA(a);
        }
    }

    public void log(StringBuilder sb) {
//        int hitCount = 0;
//
//        int readASortFileCount = 0;
//        long readASortFileTime = 0;
//        int readASortCount = 0;
//        int readFileACount = 0;
//        long readFileATime = 0;
//        int readACount = 0;
//
//        sb.append("mergeCount:").append(putCount).append(",tIndexPos:").append(tIndexPos);
//        sb.append(",tBytes:").append(tEncoder.getBitPosition() / 8).append(",tAllocMem:").append(tBuf.capacity());
//        sb.append(",firstT:").append(firstT).append(",lastT:").append(lastT);
//        sb.append("\n");
//
//        Map<Integer, Integer> map = new TreeMap<>();
//        Map<Integer, Integer> countMap = new TreeMap<>();
//        Map<Integer, Integer> intervalMap = new TreeMap<>();
//        for (GetAvgItem getItem : getItems) {
//            readASortFileCount += getItem.readASortFileCount;
//            readASortFileTime += getItem.readASortFileTime;
//            readASortCount += getItem.readASortCount;
//
//            readFileACount += getItem.readFileACount;
//            readFileATime += getItem.readFileATime;
//            readACount += getItem.readACount;
//
//            hitCount += getItem.readHitCount;
//
//            getItem.map.forEach((k, v) -> map.put(k, map.getOrDefault(k, 0) + v));
//            getItem.countMap.forEach((k, v) -> countMap.put(k, countMap.getOrDefault(k, 0) + v));
//            getItem.intervalMap.forEach((k, v) -> intervalMap.put(k, intervalMap.getOrDefault(k, 0) + v));
//
//            sb.append("readFileACount:").append(getItem.readFileACount).append(",readASortFileCount:").append(getItem.readASortFileCount)
//                    .append(",readACount:").append(getItem.readACount).append(",readASortCount:").append(getItem.readASortCount)
//                    .append(",readFileATime:").append(getItem.readFileATime).append(",readASortFileTime:").append(getItem.readASortFileTime)
//                    .append(",hitCount:").append(getItem.readHitCount).append(",accCostTime:").append(getItem.costTime)
//                    .append("\n");
//        }
//
//        AtomicInteger mapSize = new AtomicInteger();
//        map.forEach((k, v) -> {
//                    sb.append("[").append(k).append(",").append(v).append("]");
//                    mapSize.addAndGet(v);
//                }
//        );
//        sb.append("\n");
//        sb.append("----------------------------------------------countMap\n");
//        countMap.forEach((k, v) -> sb.append("[").append(k).append(",").append(v).append("]"));
//        sb.append("\n");
//        sb.append("----------------------------------------------intervalMap\n");
//        intervalMap.forEach((k, v) -> sb.append("[").append(k).append(",").append(v).append("]"));
//        sb.append("\n");
//
//        sb.append("mapSize:").append(mapSize.get())
//                .append("readFileACount:").append(readFileACount).append(",readASortFileCount:").append(readASortFileCount)
//                .append(",readACount:").append(readACount).append(",readASortCount:").append(readASortCount)
//                .append(",readFileATime:").append(readFileATime).append(",readASortFileTime:").append(readASortFileTime)
//                .append(",hitCount:").append(hitCount)
                sb.append(",MAX_T_INDEX_INTERVAL:").append(Const.MAX_T_INDEX_INTERVAL).append(",MAX_T_INDEX_LENGTH:").append(Const.MAX_T_INDEX_LENGTH)
                .append(",FILE_NUMS:").append(Const.FILE_NUMS).append(",GET_THREAD_NUM:").append(Const.GET_THREAD_NUM)
                .append(",A_INDEX_INTERVAL:").append(Const.A_INDEX_INTERVAL)
                .append(",T_INDEX_INTERVALS:").append(Arrays.toString(Const.T_INDEX_INTERVALS))
                .append("onceCount:").append(onceCounter.get()).append("\n");
//
//        aFile.log(sb);
//        for (PartitionIndex partitionIndex : partitionIndices) {
//            partitionIndex.log(sb);
//        }
    }
}
