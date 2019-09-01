package io.openmessaging.index;

import io.openmessaging.Const;
import io.openmessaging.manager.SecondFileManager;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created by yanghuiwei on 2019-08-28
 */
public class SecondTAIndex {

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
               SecondFileManager.writeASort(a[j]);
                sumA += a[j];
            }
            aSumArr.putLong(aIndexPos * Const.LONG_BYTES, sumA);
            aIndexPos++;
        }
    }

    public static void log(StringBuilder sb) {

    }
}
