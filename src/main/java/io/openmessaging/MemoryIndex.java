package io.openmessaging;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yanghuiwei on 2019-08-02
 */
public class MemoryIndex {
    static AtomicInteger indexBufCounter = new AtomicInteger();
    static AtomicInteger tBufCounter = new AtomicInteger(0);
    static AtomicInteger aBufCounter = new AtomicInteger(0);

    //put计数
    int putCount = 0;

    //索引内存数组
    //当前索引正在使用的内存，一个索引内存
    PrimaryIndex primaryIndex = new PrimaryIndex();
    int primaryIndexPos = 0;
    //indexBufs存的元素个数
    int indexBufEleCount = 0;
    int lastT;

    //t的相对值存储的内存
    Memory memory = new Memory();

    public void put(int t, int prevT, int a, int prevA) {
        lastT = t;
        //比如对于1 2 3 4 5 6，间隔为2，会存 1 3 5
        if (putCount % Const.INDEX_INTERVAL == 0) {
            indexBufEleCount++;

            int pos = primaryIndexPos++;
            //每隔INDEX_INTERVAL记录t（在indexBuf中记录了t就不会在memory中记录）
            primaryIndex.tArr[pos] = t;
            //下一个t的起始位置，先写在哪个块中，再写块的便宜位置
            primaryIndex.offsetArr[pos] = memory.putBitLength;


            primaryIndex.aArr[pos] = a;

            // 0位置记录的是[0, INDEX_INTERVAL)的区间信息
            primaryIndex.aMinArr[pos] = a;
            primaryIndex.aMaxArr[pos] = a;
            primaryIndex.aSumArr[pos] = a;

        } else {

            int diffT = t - prevT, diffA = a - prevA;

            memory.put(diffT, diffA);

            int pos = primaryIndexPos - 1;
            if (primaryIndex.aMinArr[pos] > a) {
                primaryIndex.aMinArr[pos] = a;
            }
            if (primaryIndex.aMaxArr[pos] < a) {
                primaryIndex.aMaxArr[pos] = a;
            }
            primaryIndex.aSumArr[pos] += a;
        }
        putCount++;
    }

    /**
     *
     * @param minPos >= 在PrimaryIndex的开始位置
     * @param maxPos < 在PrimaryIndex的结束位置
     * @return 返回读取的条数
     */
    public int rangePosInPrimaryIndex(int minPos, int maxPos, int[] destT, int[] destA) {
        //数据可能会存在多个块中
        int nextOffset, putBitLength = memory.putBitLength;
        int destOffset = 0;
        UnsafeMemory buf = memory.data;

        PrimaryIndex index = primaryIndex;
        while (minPos < maxPos) {
            //得到索引内存中这个位置的t值
            destT[destOffset] = index.tArr[minPos];
            destA[destOffset] = index.aArr[minPos];
            destOffset++;

            //得到这个t的下一个t在内存块的位置
            nextOffset = index.offsetArr[minPos];
            if (nextOffset >= putBitLength) {
                return destOffset;
            }

            //从变长编码内存中读
            for (int k = 1 ; k < Const.INDEX_INTERVAL; k++) {
                nextOffset = VariableUtils.getUnsigned(buf, nextOffset, destT, destOffset);
                destT[destOffset] += destT[destOffset - 1];
                nextOffset = VariableUtils.getSigned(buf, nextOffset, destA, destOffset);
                destA[destOffset] += destA[destOffset - 1];

                destOffset++;

                if (nextOffset >= putBitLength) {
                    //说明读完了
                    return destOffset;
                }
            }
            minPos++;
        }
        return destOffset;
    }

    public void sum(int minPos, int maxPos, int aMin, int aMax, int tMin, int tMax, IntervalSum res) {
        if (minPos >= maxPos) {
            return;
        }

        long sum = 0;
        int count = 0;

        PrimaryIndex index = primaryIndex;
        //读第一块
        if (!aRangeOutZone(index, minPos, aMin, aMax)) {
            sumAPosInPrimaryIndex(index, minPos, aMin, aMax, tMin, tMax, res);
        }
        minPos++;
        if (minPos < maxPos) {
            maxPos--;
            //最后一块
            if (!aRangeOutZone(index, maxPos, aMin, aMax)) {
                sumAPosInPrimaryIndex(index, maxPos, aMin, aMax, tMin, tMax, res);
            }

            int maxIndexPos = indexBufEleCount - 1;
            //中间
            while (minPos < maxPos) {
                if (aRangeInZone(index, minPos, aMin, aMax)) {
                    sum += index.aSumArr[minPos];
                    if (minPos == maxIndexPos) {
                        count += putCount % Const.INDEX_INTERVAL;
                    } else {
                        count += Const.INDEX_INTERVAL;
                    }
                } else if (!aRangeOutZone(index, minPos, aMin, aMax)) {
                    sumAPosInPrimaryIndex(index, minPos, aMin, aMax, tMin, tMax, res);
                }
                minPos++;
            }
        }
        res.sum += sum;
        res.count += count;
    }

    public void sumAPosInPrimaryIndex(PrimaryIndex index, int offset, int aMin, int aMax, int tMin, int tMax, IntervalSum res) {
        long sum = 0;
        int count = 0;

        //得到索引内存中这个位置的t值
        int t = index.tArr[offset], a = index.aArr[offset];
        if (t >= tMin && t <= tMax && a >= aMin && a <= aMax) {
            sum = a;
            count = 1;
        }

        //不是最后一个元素，得到下一个t、a在内存块的位置
        int nextOffset = index.offsetArr[offset];
        int putBitLength = memory.putBitLength;
        if (nextOffset < putBitLength) {
            UnsafeMemory buf = memory.data;
            int[] dest = new int[1];
            //从变长编码内存中读
            for (int k = 1; k < Const.INDEX_INTERVAL; k++) {
                nextOffset = VariableUtils.getUnsigned(buf, nextOffset, dest, 0);
                t += dest[0];
                nextOffset = VariableUtils.getSigned(buf, nextOffset, dest, 0);
                a += dest[0];

                if (t > tMax) {
                    break;
                }
                if (t >= tMin && a >= aMin && a <= aMax) {
                    sum += a;
                    count++;
                }

                if (nextOffset >= putBitLength) {
                    break;
                }
            }
        }
        res.sum += sum;
        res.count += count;
    }

    public boolean aRangeInZone(PrimaryIndex index, int offset, int aMin, int aMax) {
        return aMax >= index.aMaxArr[offset] && aMin <= index.aMinArr[offset];
    }

    public boolean aRangeOutZone(PrimaryIndex index, int offset, int aMin, int aMax) {
        return aMax < index.aMinArr[offset] || aMin > index.aMaxArr[offset];
    }

    public int firstGreatInPrimaryIndex(int val) {
        int low = 0, high = indexBufEleCount, mid;
        PrimaryIndex index = primaryIndex;
        while(low < high){
            mid = low + (high - low) / 2;

            if(index.tArr[mid] > val) {
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
    public int firstLessInPrimaryIndex(int val) {
        int low = 0, high = indexBufEleCount, mid;
        PrimaryIndex index = primaryIndex;

        //先找第一个大于等于val的位置，减1就是第一个小于val的位置
        while (low < high) {
            mid = low + (high - low) / 2;

            if (val > index.tArr[mid]) {
                low = mid + 1;
            }
            else {
                high = mid;
            }
        }
        return low == 0 ? low : low - 1;
    }

    public void flush() {
        Utils.print("MemoryIndex func=flush indexBuf:" + indexBufCounter.get() + " tBuf:" + tBufCounter.get() + " aBuf:" + aBufCounter.get() + " indexBufEleCount:" + indexBufEleCount
                + " putCount:" + putCount);
    }

}
