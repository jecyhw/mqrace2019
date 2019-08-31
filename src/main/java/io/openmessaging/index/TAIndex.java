package io.openmessaging.index;

import io.openmessaging.Const;
import io.openmessaging.GetItem;
import io.openmessaging.IntervalSum;
import io.openmessaging.Message;
import io.openmessaging.codec.TDecoder;
import io.openmessaging.codec.TEncoder;
import io.openmessaging.manager.FileManager;
import io.openmessaging.util.ArrayUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by yanghuiwei on 2019-08-28
 */
public class TAIndex {
    private static long[] tIndexArr = new long[Const.MERGE_T_INDEX_LENGTH];
    private static long[] tMemIndexArr = new long[Const.MERGE_T_INDEX_LENGTH];
    private static int tIndexPos = 0;

    private static final ByteBuffer tBuf = ByteBuffer.allocateDirect(Const.T_MEMORY_SIZE);
    private static TEncoder tEncoder = new TEncoder(tBuf);


    private static final long[] aIndexArr = new long[Const.A_INDEX_LENGTH];
    private static final long[] aSumArr = new long[Const.A_INDEX_LENGTH];
    private static int aIndexPos = 0;


    public static void addTIndex(long chunkPrevT) {
        int index = TAIndex.tIndexPos++;

        TAIndex.tIndexArr[index] = chunkPrevT;
        TAIndex.tMemIndexArr[index] =  TAIndex.tEncoder.getLongBitPosition();
        TAIndex.tEncoder.resetDelta();
    }

    public static void encodeDeltaT(int deltaT) {
        tEncoder.encode(deltaT);
    }



    private static ThreadLocal<GetItem> getItemThreadLocal = ThreadLocal.withInitial(() -> {
        GetItem getItem = new GetItem();
        getItem.readBuf = ByteBuffer.allocate(Const.MERGE_T_INDEX_INTERVAL * Const.MSG_BYTES);
        return getItem;
    });

    public static List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        if (aMin > aMax || tMin > tMax) {
            return Collections.emptyList();
        }

        GetItem getItem = getItemThreadLocal.get();

        List<Message> messages = new ArrayList<>(Const.MAX_GET_MESSAGE_SIZE);

        int beginTIndexPos = ArrayUtils.findFirstLessThanIndex(tIndexArr, tMin, 0, tIndexPos);
        int endTIndexPos = ArrayUtils.findFirstGreatThanIndex(tIndexArr, tMax, 0, tIndexPos);
        if (beginTIndexPos >= endTIndexPos) {
            return Collections.emptyList();
        }

        ByteBuffer tBufDup = tBuf.duplicate();
        while (beginTIndexPos < endTIndexPos) {
            //一个区间一个区间进行处理
            long[] ts = getItem.ts, as = getItem.as;
            //先读取区间里面的t，并返回读取的个数；在读取a、msg
            int readCount = readChunkT(beginTIndexPos, ts, getItem.tDecoder, tBufDup);
            //这里a和t的buf用一个
            ByteBuffer buf = getItem.readBuf;
            int beginCount = beginTIndexPos * Const.MERGE_T_INDEX_INTERVAL;
            FileManager.readChunkA(beginCount, as, readCount, buf);

            for (int i = 0; i < readCount; i++) {
                long t = ts[i], a = as[i];
                if (t >= tMin && t <= tMax && a >= aMin && a <= aMax) {
                    //readChunkMsg会把buf的position设置为0，所以可以直接读
                    messages.add(new Message(a, t, null));
                }
            }
            beginTIndexPos++;
        }

        //t有序，所以不需要排序
        return messages;
    }


    public static long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        if (aMin > aMax || tMin > tMax) {
            return 0;
        }

        GetItem getItem = getItemThreadLocal.get();

        ByteBuffer tBufDup = tBuf.duplicate();
        //对t进行精确定位，省去不必要的操作，查找的区间是左闭右开
        int beginTPos = findLeftClosedInterval(tMin, getItem.tDecoder, tBufDup);
        int endTPos = findRightOpenInterval(tMax, getItem.tDecoder, tBufDup);
        if (beginTPos >= endTPos) {
            System.out.println("1.begin[" + beginTPos + "," + tMin + "],end[" + endTPos + "," + tMax + "]");
            return 0;
        }

        IntervalSum intervalSum = getItem.intervalSum;
        intervalSum.reset();

        long sum = 0;
        int count = 0;

        long[] as = getItem.as;
        ByteBuffer readBuf = getItem.readBuf;

        //处理首区间
        int beginTIndexPos = beginTPos / Const.MERGE_T_INDEX_INTERVAL;
        //处理最后尾区间，最后一个区间如果个数不等于Const.FIXED_CHUNK_SIZE，肯定会在这里处理
        int firstChunkFilterReadCount = beginTPos % Const.MERGE_T_INDEX_INTERVAL;

        int endTIndexPos = endTPos / Const.MERGE_T_INDEX_INTERVAL;
        int lastChunkNeedReadCount = endTPos % Const.MERGE_T_INDEX_INTERVAL;

        //只有一个区间
        if (beginTIndexPos == endTIndexPos) {
            //读取按t分区的首区间剩下的a的数量
            int firstChunkNeedReadCount = lastChunkNeedReadCount - firstChunkFilterReadCount;
            FileManager.readChunkA(beginTPos, as, firstChunkNeedReadCount, readBuf);
            sumChunkA(as, firstChunkNeedReadCount, aMin, aMax, intervalSum);

            System.out.println("2.sum:" + intervalSum.sum + ",count:" + intervalSum.count + ",avg:" + intervalSum.avg());
            return intervalSum.avg();
        }

        //至少两个分区，先处理首尾分区
        if (firstChunkFilterReadCount > 0) {
            //读取按t分区的首区间剩下的a的数量
            int firstChunkNeedReadCount = Const.MERGE_T_INDEX_INTERVAL - firstChunkFilterReadCount;
            FileManager.readChunkA(beginTPos, as, firstChunkNeedReadCount, readBuf);
            sumChunkA(as, firstChunkNeedReadCount, aMin, aMax, intervalSum);

            beginTIndexPos++;
        }


        if (lastChunkNeedReadCount > 0) {
            //读取按t分区的尾区间里面的a
            FileManager.readChunkA(endTIndexPos * Const.MERGE_T_INDEX_INTERVAL, as, lastChunkNeedReadCount, readBuf);
            sumChunkA(as, lastChunkNeedReadCount, aMin, aMax, intervalSum);
        }

        //首尾区间处理之后，[beginTIndexPos, endTIndexPos)中的t都是符合条件，不用再判断
        while (beginTIndexPos < endTIndexPos) {

            //t区间内对a进行二分查询
            int low = beginTIndexPos * (Const.MERGE_T_INDEX_INTERVAL / Const.A_INDEX_INTERVAL), high = low + (Const.MERGE_T_INDEX_INTERVAL / Const.A_INDEX_INTERVAL);
            int beginASortIndexPos = ArrayUtils.findFirstLessThanIndex(aIndexArr, aMin, low, high);
            int endASortIndexPos = ArrayUtils.findFirstGreatThanIndex(aIndexArr, aMax, low, high);

            //区间内没有符合条件的a
            if (beginASortIndexPos >= endASortIndexPos) {
                beginTIndexPos++;
                continue;
            }

            //只有一块或者首尾相连时 beginASortIndexPos=1,endASortIndexPos=2表示只有1块；beginASortIndexPos=1,endASortIndexPos=3表示只有2块，属于首尾相连
            if (beginASortIndexPos + 2 >= endASortIndexPos) {
                if (beginASortIndexPos == endASortIndexPos + 1) {
                    //只有一块，并且这一块只有部分满足，才需要读取这一块
                    if (beginASortIndexPos > low || aIndexArr[endASortIndexPos] > aMax) {
                        FileManager.readChunkA(beginASortIndexPos * Const.A_INDEX_INTERVAL, as, Const.A_INDEX_INTERVAL, readBuf);
                        sumChunkA(as, Const.A_INDEX_INTERVAL, aMin, aMax, intervalSum);
                        beginTIndexPos++;
                        continue;
                    }
                } else {
                    //有两块
                    if (beginASortIndexPos == low) {
                        //第一块的全部满足，看第二块
                        if (aIndexArr[endASortIndexPos] > aMax) {
                            //第二块部分部分满足
                            endASortIndexPos--;
                            FileManager.readChunkA(endASortIndexPos * Const.A_INDEX_INTERVAL, as, Const.A_INDEX_INTERVAL, readBuf);
                            sumChunkA(as, Const.A_INDEX_INTERVAL, aMin, aMax, intervalSum);
                        }
                    } else if (aIndexArr[endASortIndexPos] <= aMax) {
                        //第一块部分满足，第二块全部满足，读取第一块
                        FileManager.readChunkA(beginASortIndexPos * Const.A_INDEX_INTERVAL, as, Const.A_INDEX_INTERVAL, readBuf);
                        sumChunkA(as, Const.A_INDEX_INTERVAL, aMin, aMax, intervalSum);
                    } else {
                        //第一块和第二块都是部分满足，读取两块
                        FileManager.readChunkA(beginASortIndexPos * Const.A_INDEX_INTERVAL, as, Const.A_INDEX_INTERVAL * 2, readBuf);
                        sumChunkA(as, Const.A_INDEX_INTERVAL * 2, aMin, aMax, intervalSum);
                        beginTIndexPos += 2;
                        continue;
                    }
                }
            } else {
                if (beginASortIndexPos > low) {
                    //读取第一个a区间内的的所有a
                    FileManager.readChunkASort(beginASortIndexPos * Const.A_INDEX_INTERVAL, as, Const.A_INDEX_INTERVAL, readBuf);
                    sumChunkA(as, Const.A_INDEX_INTERVAL, aMin, aMax, intervalSum);
                    ++beginASortIndexPos;
                }

                if (aIndexArr[endASortIndexPos] > aMax) {
                    //读取最后一个a区间内的所有a
                    endASortIndexPos--;
                    FileManager.readChunkASort(endASortIndexPos * Const.A_INDEX_INTERVAL, as, Const.A_INDEX_INTERVAL, readBuf);
                    sumChunkA(as, Const.A_INDEX_INTERVAL, aMin, aMax, intervalSum);
                }
            }

            // 经过上面处理之后，[beginASortIndexPos, endASortIndexPos)都是符合条件的，直接累加
            while (beginASortIndexPos < endASortIndexPos) {
                sum += aSumArr[beginASortIndexPos];
                count += Const.A_INDEX_INTERVAL;
                beginASortIndexPos++;
            }

            beginTIndexPos++;
        }
        intervalSum.add(sum, count);
        System.out.println("3.sum:" + intervalSum.sum + ",count:" + intervalSum.count + ",avg:" + intervalSum.avg());
        return intervalSum.avg();
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


    private static boolean isError = true;
    public static void flush(long t[], long a[], int len) {
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
            if (curT - prevT > 1) {
                if (isError) {
                    System.out.println("curT:" + curT + ",prevT:" + prevT);
                    isError = false;
                }
            }
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
        //按照a进行排序
        Arrays.parallelSort(a, 0, chunkSize);

        //在t的基础上在建立分区
        for (int i = 0; i < chunkSize; i += Const.A_INDEX_INTERVAL) {
            aIndexArr[aIndexPos] = a[i];

            int end = Math.min(i + Const.A_INDEX_INTERVAL, chunkSize);
            for (int j = i; j < end; j++) {
                FileManager.writeASort(a[j]);
                aSumArr[aIndexPos] += a[j];
            }
            aIndexPos++;
        }
    }

    public static void flushEnd(long t[], long a[], int len) {
        if (len > 0) {
            flush(t, a, len);
        }
        FileManager.flushEnd();
        tEncoder.flush();
    }

    public static void log(StringBuilder sb) {
        sb.append("mergeCount:").append(putCount).append(",tIndexPos:").append(tIndexPos).append(",aIndexPos:").append(aIndexPos);
        sb.append(",tBytes:").append(tEncoder.getLongBitPosition() / 8).append(",tAllocMem:").append(tBuf.capacity());
        sb.append(",firstT:").append(firstT).append(",lastT:").append(lastT);
        sb.append("\n");
    }
}
