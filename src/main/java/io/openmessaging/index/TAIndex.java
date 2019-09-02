package io.openmessaging.index;

import io.openmessaging.Const;
import io.openmessaging.codec.TDecoder;
import io.openmessaging.codec.TEncoder;
import io.openmessaging.model.GetAvgItem;
import io.openmessaging.model.IntervalSum;
import io.openmessaging.partition.PartitionFile;
import io.openmessaging.partition.PartitionIndex;
import io.openmessaging.util.ArrayUtils;
import io.openmessaging.util.ByteBufferUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yanghuiwei on 2019-08-28
 */
public class TAIndex {
    public static TAIndex taIndex = new TAIndex();

    private final long[] tIndexArr = new long[Const.MERGE_T_INDEX_LENGTH];
    private final int[] tMemIndexArr = new int[Const.MERGE_T_INDEX_LENGTH];
    private int tIndexPos = 0;


    private final ByteBuffer tBuf = ByteBuffer.allocate(Const.T_MEMORY_SIZE);
    private final TEncoder tEncoder = new TEncoder(tBuf);

    private final PartitionIndex primaryPartitionIndex;
    private PartitionFile aFile;
    private final int interval = Const.MERGE_T_INDEX_INTERVAL;

    public TAIndex() {
        PartitionIndex minPartitionIndex = new PartitionIndex(Const.MIN_T_INDEX_INTERVAL);
        int interval = Const.MIN_T_INDEX_INTERVAL * 2;

        primaryPartitionIndex = new PartitionIndex(minPartitionIndex, interval);
        aFile = new PartitionFile(interval, Const.M_A_FILE_SUFFIX);
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

    private static ThreadLocal<GetAvgItem> getItemThreadLocal = ThreadLocal.withInitial(() -> {
        GetAvgItem getItem = new GetAvgItem();
        synchronized (TAIndex.class) {
            getItems.add(getItem);
        }
        return getItem;
    });

    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
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

        //处理首区间
        int beginPartition = beginTPos / interval;
        //处理最后尾区间，最后一个区间如果个数不等于Const.FIXED_CHUNK_SIZE，肯定会在这里处理
        int endPartition = endTPos / interval;

        //只有一个区间
        if (beginPartition == endPartition) {
            return avgFromOnePartition(beginTPos, endTPos, beginPartition, aMin, aMax, getItem);
        }

        int firstPartitionFilterCount = beginTPos % interval;
        int lastChunkNeedReadCount = endTPos % interval;
        long sum = 0;
        int count = 0;
        //至少两个分区，先处理首尾分区
        if (firstPartitionFilterCount > 0) {
            sumPartitionRightClosed(beginPartition, firstPartitionFilterCount, aMin, aMax, getItem);
            beginPartition++;
        }

        if (lastChunkNeedReadCount > 0) {
            sumPartitionLeftClosed(endPartition, lastChunkNeedReadCount, aMin, aMax, getItem);
        }

        //首尾区间处理之后，[beginPartition, endPartition)中的t都是符合条件，不用再判断
        while (beginPartition < endPartition) {
            primaryPartitionIndex.partitionSum(beginPartition, aMin, aMax, getItem);
            beginPartition++;
        }
        intervalSum.add(sum, count);

        getItem.costTime += (System.currentTimeMillis() - startTime);
        getItem.readHitCount += intervalSum.count;

        return intervalSum.avg();
    }

    private void sumPartitionRightClosed(int partition, int partitionFilterCount, long aMin, long aMax, GetAvgItem getItem) {
        int partitionNeedCount = interval - partitionFilterCount;
        int levelPartition = partition;
        PartitionIndex nextPartitionIndex = primaryPartitionIndex.getNextPartitionIndex();

        while (true) {
            //读取个数为0
            if (partitionNeedCount == 0) {
                return;
            }

            //分层索引结束
            if (nextPartitionIndex == null) {
                readAndSumFromAPartition(partition, partitionFilterCount, partitionNeedCount, aMin, aMax, getItem);
                return;
            }

            //要读取的个数小于分层的区间大小
            int nextInterval = nextPartitionIndex.getInterval();
            if (partitionNeedCount < nextInterval) {
                readAndSumFromAPartition(partition, partitionFilterCount, partitionNeedCount, aMin, aMax, getItem);
                return;
            }

            levelPartition <<= 1;
            //去下一层读
            nextPartitionIndex.partitionSum(levelPartition + 1, aMin, aMax, getItem);
            nextPartitionIndex = nextPartitionIndex.getNextPartitionIndex();

            partitionNeedCount -= nextInterval;
        }
    }

    private void sumPartitionLeftClosed(int partition, int partitionNeedCount, long aMin, long aMax, GetAvgItem getItem) {
        PartitionIndex nextPartitionIndex = primaryPartitionIndex.getNextPartitionIndex();
        int levelPartition = partition;
        int offsetCount = 0;

        while (true) {
            //读取个数为0
            if (partitionNeedCount == 0) {
                return;
            }

            //分层索引结束
            if (nextPartitionIndex == null) {
                readAndSumFromAPartition(partition, offsetCount, partitionNeedCount - offsetCount, aMin, aMax, getItem);
                return;
            }

            //要读取的个数小于分层的区间大小
            int nextInterval = nextPartitionIndex.getInterval();
            if (partitionNeedCount < nextInterval) {
                readAndSumFromAPartition(partition, offsetCount, partitionNeedCount - offsetCount, aMin, aMax, getItem);
                return;
            }

            levelPartition <<= 1;
            //去下一层读
            nextPartitionIndex.partitionSum(levelPartition, aMin, aMax, getItem);
            nextPartitionIndex = nextPartitionIndex.getNextPartitionIndex();

            offsetCount += nextInterval;
        }
    }

    private void readAndSumFromAPartition(int partition, int offsetCount, int readCount, long aMin, long aMax, GetAvgItem getItem) {
        ByteBuffer readBuf = getItem.readBuf;
        //读取按t分区的首区间剩下的a的数量
        aFile.readPartition(partition, offsetCount, readCount, readBuf, getItem);
        ByteBufferUtil.sumChunkA(readBuf, readCount, aMin, aMax, getItem.intervalSum);

        getItem.map.put(readCount, getItem.map.getOrDefault(readCount, 0) + 1);
    }


    private long avgFromOnePartition(int beginTPos, int endTPos, int beginPartition, long aMin, long aMax, GetAvgItem getItem) {
        int firstPartitionFilterCount = beginTPos % interval;
        int lastChunkNeedReadCount = endTPos % interval;
        ByteBuffer readBuf = getItem.readBuf;
        IntervalSum intervalSum = getItem.intervalSum;

        //读取按t分区的首区间剩下的a的数量
        int firstChunkNeedReadCount = lastChunkNeedReadCount - firstPartitionFilterCount;
        aFile.readPartition(beginPartition, firstPartitionFilterCount, firstChunkNeedReadCount, readBuf, getItem);
        ByteBufferUtil.sumChunkA(readBuf, firstChunkNeedReadCount, aMin, aMax, intervalSum);

        getItem.map.put(firstChunkNeedReadCount, getItem.map.getOrDefault(firstPartitionFilterCount, 0) + 1);
        return intervalSum.avg();
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
            return beginTIndexOffset * Const.MERGE_T_INDEX_INTERVAL;
        }
        return tDecoder.getFirstGreatOrEqual(tBufDup, t, destT, beginTIndexOffset * Const.MERGE_T_INDEX_INTERVAL + 1, tMemIndexArr[beginTIndexOffset]);
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
            return beginTIndexOffset * Const.MERGE_T_INDEX_INTERVAL;
        }
        int pos = tDecoder.getFirstGreat(tBufDup, t, destT, beginTIndexOffset * Const.MERGE_T_INDEX_INTERVAL + 1, tMemIndexArr[beginTIndexOffset]);
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

        completeASortAndCreateIndex(a, len);

        //更新putCount
        putCount += len;

        //更新最后一个t
        lastT = t[len - 1];
    }

    private void completeASortAndCreateIndex(long[] a, int chunkSize) {
        primaryPartitionIndex.createPartition(a, 0, chunkSize);
    }

    public void flush(long[] t, long[] a, int len) {
        if (len > 0) {
            createIndex(t, a, len);
        }
        aFile.flush();
        tEncoder.flush();
        primaryPartitionIndex.flush();
    }

    public void log(StringBuilder sb) {
        int readChunkAFileCount = 0, readChunkASortFileCount = 0, sumChunkASortFileCount = 0;
        int readChunkACount = 0, readChunkASortCount = 0, sumChunkASortCount = 0;
        int hitCount = 0;

        sb.append("mergeCount:").append(putCount).append(",tIndexPos:").append(tIndexPos);
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
    }
}
