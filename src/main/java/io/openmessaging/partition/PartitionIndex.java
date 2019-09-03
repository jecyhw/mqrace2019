package io.openmessaging.partition;

import io.openmessaging.Const;
import io.openmessaging.model.GetAvgItem;
import io.openmessaging.model.IntervalSum;
import io.openmessaging.util.ArrayUtils;
import io.openmessaging.util.ByteBufferUtil;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yanghuiwei on 2019-09-02
 */
public final class PartitionIndex {
    private final ByteBuffer aIndexArr = ByteBuffer.allocateDirect(Const.A_INDEX_LENGTH * 8);
    private final ByteBuffer aSumArr = ByteBuffer.allocateDirect(Const.A_INDEX_LENGTH * 8);
    private int aIndexPos = 0;

    private final int interval;
    private final MultiPartitionFile partitionFile;

    private final long[] as;
    private int asIndex = 0;
    private AtomicInteger hitCounter = new AtomicInteger(0);

    public PartitionIndex(int interval) {
        this.interval = interval;
        partitionFile = new MultiPartitionFile(interval, Const.M_A_FILE_SUFFIX + interval);
        as = new long[interval];
    }

    public void partitionSum(int partition, long aMin, long aMax, GetAvgItem getItem) {
        hitCounter.incrementAndGet();

        ByteBuffer aIndexBuf = aIndexArr.duplicate();
        //t区间内对a进行二分查询
        int low = partition * (interval / Const.A_INDEX_INTERVAL);
        int high = low + interval / Const.A_INDEX_INTERVAL;
        int beginPartition = ArrayUtils.findFirstLessThanIndex(aIndexBuf, aMin, low, high);
        int endPartition = ArrayUtils.findFirstGreatThanIndex(aIndexBuf, aMax, low, high);

        //区间内没有符合条件的a
        if (beginPartition >= endPartition) {
            return;
        }

        ByteBuffer readBuf = getItem.readBuf;
        IntervalSum intervalSum = getItem.intervalSum;

        //只有一块或者首尾相连时 beginPartition=1,endPartition=2表示只有1块；beginPartition=1,endPartition=3表示只有2块，属于首尾相连
        if (beginPartition + 2 >= endPartition) {
            //小于等于两块，一次读取
            int chunkCount = endPartition - beginPartition;
            int readCount = Const.A_INDEX_INTERVAL * chunkCount;
            partitionFile.readPartition(partition,  (beginPartition - low) * Const.A_INDEX_INTERVAL, readCount, readBuf, getItem);
            ByteBufferUtil.sumChunkA(readBuf, readCount, aMin, aMax, intervalSum);

            getItem.readASortFileCount++;
            getItem.readACount += readCount;
        } else {
            if (aIndexBuf.getLong(beginPartition * Const.LONG_BYTES) < aMin) {
                //读取第一个a区间内的的所有a
                partitionFile.readPartition(partition, (beginPartition - low) * Const.A_INDEX_INTERVAL, Const.A_INDEX_INTERVAL, readBuf, getItem);
                ByteBufferUtil.sumChunkA(readBuf, Const.A_INDEX_INTERVAL, aMin, aMax, intervalSum);
                ++beginPartition;
            }

            if (aIndexBuf.getLong(endPartition * Const.LONG_BYTES) > aMax) {
                //读取最后一个a区间内的所有a
                endPartition--;
                partitionFile.readPartition(partition, (endPartition - low) * Const.A_INDEX_INTERVAL, Const.A_INDEX_INTERVAL, readBuf, getItem);
                ByteBufferUtil.sumChunkA(readBuf, Const.A_INDEX_INTERVAL, aMin, aMax, intervalSum);
            }
            long sum = 0;
            int count = 0;
            ByteBuffer aSumBuf = aSumArr.duplicate();
            // 经过上面处理之后，[beginPartition, endPartition)都是符合条件的，直接累加
            while (beginPartition < endPartition) {
                sum += aSumBuf.getLong(beginPartition * Const.LONG_BYTES);
                count += Const.A_INDEX_INTERVAL;
                beginPartition++;
            }
            intervalSum.add(sum, count);
            getItem.readHitCount += count;
        }
    }

    public void createPartition(int fromIndex, int toIndex) {
        //按照a进行排序
        Arrays.parallelSort(as, fromIndex, toIndex);
        //在t的基础上在建立分区
        for (int i = fromIndex; i < toIndex; i += Const.A_INDEX_INTERVAL) {
            aIndexArr.putLong(aIndexPos * Const.LONG_BYTES, as[i]);
            long sumA = 0;
            int end = Math.min(i + Const.A_INDEX_INTERVAL, toIndex);
            for (int j = i; j < end; j++) {
                partitionFile.writeA(as[j]);
                sumA += as[j];
            }
            aSumArr.putLong(aIndexPos * Const.LONG_BYTES, sumA);
            aIndexPos++;
        }

        aSumArr.putLong(aIndexPos * Const.LONG_BYTES, as[toIndex - 1]);
    }

    public void flush() {
        partitionFile.flush();
        if (asIndex > 0) {
            createPartition(0, asIndex);
        }
    }



    public int getInterval() {
        return interval;
    }

    public void putA(long a) {
        as[asIndex++] = a;

        if (asIndex == interval) {
            createPartition(0, interval);
            asIndex = 0;
        }
    }


    public void log(StringBuilder sb) {
        sb.append("interval:").append(interval).append(",hitCount:").append(hitCounter.get()).append("\n");
    }
}
