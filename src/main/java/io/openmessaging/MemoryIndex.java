package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yanghuiwei on 2019-08-02
 */
public class MemoryIndex {
    static AtomicInteger indexBufCounter = new AtomicInteger();
    static AtomicInteger tBufCounter = new AtomicInteger(1);
    static AtomicInteger aBufCounter = new AtomicInteger(1);

    //put计数
    int putCount = 0;

    //索引内存数组
    List<PrimaryIndex> primaryIndices = new ArrayList<>();
    //当前索引正在使用的内存
    PrimaryIndex primaryIndex;
    int primaryIndexPos = 0;
    //indexBufs存的元素个数
    int indexBufEleCount = 0;
    int lastT;

    //t的相对值存储的内存
    List<Memory> memories = new ArrayList<>();
    Memory memory;

    public MemoryIndex() {
        newMemory();
        newPrimaryIndex();
    }

    public void put(int t, int prevT, int a, int prevA) {
        lastT = t;
        //比如对于1 2 3 4 5 6，间隔为2，会存 1 3 5
        if (putCount % Const.INDEX_INTERVAL == 0) {
            if (primaryIndexPos == Const.INDEX_ELE_LENGTH) {
                newPrimaryIndex();
                primaryIndexPos = 0;
            }
            indexBufEleCount++;

            int pos = primaryIndexPos++;
            //每隔INDEX_INTERVAL记录t（在indexBuf中记录了t就不会在memory中记录）
            primaryIndex.tArr[pos] = t;
            //下一个t的起始位置，先写在哪个块中，再写块的便宜位置
            if (memory.hasRemaining()) {
                primaryIndex.posArr[pos] = (short) (memories.size() - 1);
                primaryIndex.offsetArr[pos] = memory.putBitLength;
            } else {
                primaryIndex.posArr[pos] = (short) memories.size();
                primaryIndex.offsetArr[pos] = 0;
            }

            primaryIndex.aArr[pos] = a;

            // 0位置记录的是[0, INDEX_INTERVAL)的区间信息
            primaryIndex.aMinArr[pos] = a;
            primaryIndex.aMaxArr[pos] = a;
            primaryIndex.aSumArr[pos] = a;

        } else {

            int diffT = t - prevT, diffA = a - prevA;

            if (!memory.put(diffT, diffA)) {
                newMemory();
                memory.put(diffT, diffA);
            }

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
        int nextPos = -1, nextOffset = 0, putBitLength = 0;
        int destOffset = 0;
        Memory mem;
        ByteBuffer buf = null;

        while (minPos < maxPos) {
            //得到在第几个主内存索引中
            PrimaryIndex index = primaryIndices.get(minPos / Const.INDEX_ELE_LENGTH);
            //得到在主内存索引的偏移位置
            int offset = minPos % Const.INDEX_ELE_LENGTH;
            minPos++;

            //得到索引内存中这个位置的t值
            destT[destOffset] = index.tArr[offset];
            destA[destOffset] = index.aArr[offset];
            destOffset++;

            //得到这个t的下一个t在内存块的位置
            int newNextPos = index.posArr[offset];

            if (newNextPos != nextPos) {
                if (newNextPos >= memories.size()) {
                    //说明读完了
                    return destOffset;
                }
                //内存块发生改变，更新信息
                nextPos = newNextPos;
                mem = memories.get(nextPos);
                buf = mem.data;
                nextOffset = index.offsetArr[offset];
                putBitLength = mem.putBitLength;
            }

            //从变长编码内存中读
            for (int k = 1 ; k < Const.INDEX_INTERVAL; k++) {
                nextOffset = VariableUtils.getUnsigned(buf, nextOffset, destT, destOffset);
                destT[destOffset] += destT[destOffset - 1];
                nextOffset = VariableUtils.getSigned(buf, nextOffset, destA, destOffset);
                destA[destOffset] += destA[destOffset - 1];

                destOffset++;

                if (nextOffset >= putBitLength) {
                    nextPos++;
                    if (nextPos >= memories.size()) {
                        //说明读完了
                        return destOffset;
                    }

                    nextOffset = 0;
                    mem = memories.get(nextPos);
                    buf = mem.data;
                    putBitLength = mem.putBitLength;
                }
            }
        }
        return destOffset;
    }

    public void sum(int minPos, int aMin, int aMax, int tMin, int tMax, IntervalSum res) {
        int maxPos = indexBufEleCount;
        if (minPos >= maxPos) {
            return;
        }
        long sum = 0;
        int count = 0;

        while (minPos < maxPos) {
            PrimaryIndex index = primaryIndices.get(minPos / Const.INDEX_ELE_LENGTH);
            int offset = minPos % Const.INDEX_ELE_LENGTH;
            //t在区间外
            if (tRangeOutZone(index, minPos, offset, tMin, tMax)) {
                break;
            }
            if (aRangeInZone(index, offset, aMin, aMax) && tRangeInZone(index, minPos, offset, tMin, tMax)) {
                sum += index.aSumArr[offset];
                if (minPos == indexBufEleCount - 1) {
                    count += putCount % Const.INDEX_INTERVAL;
                } else {
                    count += Const.INDEX_INTERVAL;
                }
            } else if (!aRangeOutZone(index, offset, aMin, aMax)) {
                sumAPosInPrimaryIndex(index, offset, aMin, aMax, tMin, tMax, res);
            }
            minPos++;
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

        int nextPos = index.posArr[offset];
        if (nextPos < memories.size()) {
            //不是最后一个元素，得到下一个t、a在内存块的位置

            Memory mem = memories.get(nextPos);
            ByteBuffer buf = mem.data;
            int putBitLength = mem.putBitLength;
            int nextOffset = index.offsetArr[offset];

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
                    nextPos++;
                    if (nextPos >= memories.size()) {
                        //说明读完了
                        break;
                    }

                    nextOffset = 0;
                    mem = memories.get(nextPos);
                    buf = mem.data;
                    putBitLength = mem.putBitLength;
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

    public boolean tRangeInZone(PrimaryIndex index, int minPos, int offset, int tMin, int tMax) {
        if (minPos < indexBufEleCount - 1) {
            int max;
            if (offset + 1 == Const.INDEX_ELE_LENGTH) {
                max = primaryIndices.get((minPos + 1) / Const.INDEX_ELE_LENGTH).tArr[0];
            } else {
                max = index.tArr[offset + 1];
            }
            return tMin <= index.tArr[offset] && tMax >= max;
        } else {
            //最后一个元素分开判断
            return tMin <= index.tArr[offset] && tMax >= lastT;
        }
    }

    public boolean tRangeOutZone(PrimaryIndex index, int minPos, int offset, int tMin, int tMax) {
        return tMax < index.tArr[offset];
    }

    public int firstGreatInPrimaryIndex(int val) {
        int low = 0, high = indexBufEleCount, mid, t;
        while(low < high){
            mid = low + (high - low) / 2;
            t = primaryIndices.get(mid / Const.INDEX_ELE_LENGTH).tArr[mid % Const.INDEX_ELE_LENGTH];

            if(t > val) {
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
        int low = 0, high = indexBufEleCount, mid, t;

        //先找第一个大于等于val的位置，减1就是第一个小于val的位置
        while (low < high) {
            mid = low + (high - low) / 2;
            t = primaryIndices.get(mid / Const.INDEX_ELE_LENGTH).tArr[mid % Const.INDEX_ELE_LENGTH];

            if (val > t) {
                low = mid + 1;
            }
            else {
                high = mid;
            }
        }
        return low == 0 ? low : low - 1;
    }

    public void flush() {
        Utils.print("MemoryIndex func=flush indexBuf=" + indexBufCounter.get() + " tBuf=" + tBufCounter.get() + " aBuf:" + aBufCounter.get());
    }

    private void newPrimaryIndex() {
        primaryIndex = new PrimaryIndex();
        primaryIndices.add(primaryIndex);
        indexBufCounter.getAndIncrement();
    }

    private void newMemory() {
        memory = new Memory();
        memories.add(memory);
        tBufCounter.incrementAndGet();
    }
}
