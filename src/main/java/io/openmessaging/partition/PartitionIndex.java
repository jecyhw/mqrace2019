package io.openmessaging.partition;

import io.openmessaging.Const;
import io.openmessaging.model.GetAvgItem;
import io.openmessaging.model.IntervalSum;
import io.openmessaging.util.ArrayUtils;
import io.openmessaging.util.ByteBufferUtil;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created by yanghuiwei on 2019-09-02
 */
public final class PartitionIndex {
    private final ByteBuffer aIndexArr = ByteBuffer.allocateDirect(Const.A_INDEX_LENGTH * 8);
    private final ByteBuffer aSumArr = ByteBuffer.allocateDirect(Const.A_INDEX_LENGTH * 8);
    private int aIndexPos = 0;

    private final int interval;
    private final PartitionFile partitionFile;

    private final long[] as;
    private int asIndex = 0;

    public PartitionIndex(int interval) {
        this.interval = interval;
        partitionFile = new PartitionFile(interval, Const.M_A_FILE_SUFFIX + interval);
        as = new long[interval];
    }

    public void partitionSum(int partition, long aMin, long aMax, GetAvgItem getItem) {
        ByteBuffer aIndexBuf = aIndexArr.duplicate();
        //t区间内对a进行二分查询
        int low = partition * (interval / Const.A_INDEX_INTERVAL);
        int high = low + interval / Const.A_INDEX_INTERVAL;
        int beginASortIndexPos = ArrayUtils.findFirstLessThanIndex(aIndexBuf, aMin, low, high);
        int endASortIndexPos = ArrayUtils.findFirstGreatThanIndex(aIndexBuf, aMax, low, high);

        //区间内没有符合条件的a
        if (beginASortIndexPos >= endASortIndexPos) {
            return;
        }

        ByteBuffer readBuf = getItem.readBuf;
        IntervalSum intervalSum = getItem.intervalSum;

        //只有一块或者首尾相连时 beginASortIndexPos=1,endASortIndexPos=2表示只有1块；beginASortIndexPos=1,endASortIndexPos=3表示只有2块，属于首尾相连
        if (beginASortIndexPos + 2 >= endASortIndexPos) {
            //小于等于两块，一次读取
            int chunkCount = endASortIndexPos - beginASortIndexPos;
            int readCount = Const.A_INDEX_INTERVAL * chunkCount;
            partitionFile.readPartition(partition,  (beginASortIndexPos - low) * Const.A_INDEX_INTERVAL, readCount, readBuf, getItem);
            ByteBufferUtil.sumChunkA(readBuf, readCount, aMin, aMax, intervalSum);

            getItem.readChunkASortFileCount++;
            getItem.readChunkASortCount += readCount;
        } else {
            //读取第一个a区间内的的所有a
            partitionFile.readPartition(partition, (beginASortIndexPos - low) * Const.A_INDEX_INTERVAL, Const.A_INDEX_INTERVAL, readBuf, getItem);
            ByteBufferUtil.sumChunkA(readBuf, Const.A_INDEX_INTERVAL, aMin, aMax, intervalSum);
            ++beginASortIndexPos;

            //读取最后一个a区间内的所有a
            endASortIndexPos--;
            partitionFile.readPartition(partition, (endASortIndexPos - low) * Const.A_INDEX_INTERVAL, Const.A_INDEX_INTERVAL, readBuf, getItem);
            ByteBufferUtil.sumChunkA(readBuf, Const.A_INDEX_INTERVAL, aMin, aMax, intervalSum);

            getItem.readChunkASortFileCount += 2;
            getItem.readChunkASortCount += Const.A_INDEX_INTERVAL * 2;

            getItem.sumChunkASortFileCount += (endASortIndexPos - beginASortIndexPos);
            getItem.sumChunkASortCount += (endASortIndexPos - beginASortIndexPos) * Const.A_INDEX_INTERVAL;

            long sum = 0;
            int count = 0;
            ByteBuffer aSumBuf = aSumArr.duplicate();
            // 经过上面处理之后，[beginASortIndexPos, endASortIndexPos)都是符合条件的，直接累加
            while (beginASortIndexPos < endASortIndexPos) {
                sum += aSumBuf.getLong(beginASortIndexPos * Const.LONG_BYTES);
                count += Const.A_INDEX_INTERVAL;
                beginASortIndexPos++;
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
}
