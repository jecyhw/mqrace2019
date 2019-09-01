package io.openmessaging.index;

import io.openmessaging.Const;
import io.openmessaging.manager.ThreeFileManager;
import io.openmessaging.model.GetAvgItem;
import io.openmessaging.model.IntervalSum;
import io.openmessaging.util.ArrayUtils;
import io.openmessaging.util.ByteBufferUtil;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created by yanghuiwei on 2019-08-28
 */
public class ThreeTAIndex {

    private static final ByteBuffer aIndexArr = ByteBuffer.allocateDirect(Const.A_INDEX_LENGTH * 8);
    private static final ByteBuffer aSumArr = ByteBuffer.allocateDirect(Const.A_INDEX_LENGTH * 8);
    private static int aIndexPos = 0;

    public static void completeASortAndCreateIndex(long[] a, int fromIndex, int toIndex) {
        //按照a进行排序
        Arrays.parallelSort(a, fromIndex, toIndex);

        //在t的基础上在建立分区
        for (int i = fromIndex; i < toIndex; i += Const.A_INDEX_INTERVAL) {
            aIndexArr.putLong(aIndexPos * Const.LONG_BYTES, a[i]);
            long sumA = 0;
            int end = Math.min(i + Const.A_INDEX_INTERVAL, toIndex);
            for (int j = i; j < end; j++) {
               ThreeFileManager.writeASort(a[j]);
                sumA += a[j];
            }
            aSumArr.putLong(aIndexPos * Const.LONG_BYTES, sumA);
            aIndexPos++;
        }
    }

    public static void log(StringBuilder sb) {

    }

    public static void sumThreeChunkA(int beginIndex, long aMin, long aMax, GetAvgItem getItem) {
        ByteBuffer aIndexBuf = aIndexArr.duplicate();
        //t区间内对a进行二分查询
        int low = beginIndex * (Const.THREE_MERGE_T_INDEX_INTERVAL / Const.A_INDEX_INTERVAL);
        int high = low + Const.THREE_MERGE_T_INDEX_INTERVAL / Const.A_INDEX_INTERVAL;
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
            ThreeFileManager.readChunkASort(beginASortIndexPos * Const.A_INDEX_INTERVAL, readCount, readBuf, getItem);
            ByteBufferUtil.sumChunkA(readBuf, readCount, aMin, aMax, intervalSum);

            getItem.readChunkASortFileCount++;
            getItem.readChunkASortCount += readCount;
        } else {
            //读取第一个a区间内的的所有a
            ThreeFileManager.readChunkASort(beginASortIndexPos * Const.A_INDEX_INTERVAL, Const.A_INDEX_INTERVAL, readBuf, getItem);
            ByteBufferUtil.sumChunkA(readBuf, Const.A_INDEX_INTERVAL, aMin, aMax, intervalSum);
            ++beginASortIndexPos;

            //读取最后一个a区间内的所有a
            endASortIndexPos--;
            ThreeFileManager.readChunkASort(endASortIndexPos * Const.A_INDEX_INTERVAL, Const.A_INDEX_INTERVAL, readBuf, getItem);
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
        }
    }
}
