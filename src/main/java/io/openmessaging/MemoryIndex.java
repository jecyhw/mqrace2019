package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yanghuiwei on 2019-08-02
 */
public class MemoryIndex {
    static AtomicInteger memoryIndexBufCounter = new AtomicInteger();
    static AtomicInteger memoryBufCounter = new AtomicInteger();

    //put计数
    int putCount = 0;

    //索引内存数组
    List<ByteBuffer> indexBufs = new ArrayList<>();
    //当前索引正在使用的内存
    ByteBuffer indexBuf;
    //indexBufs存的元素个数
    int indexBufEleCount = 0;

    //t的相对值存储的内存
    List<Memory> memories = new ArrayList<>();
    Memory memory = new Memory();
    int memoryPos = -1;

    public MemoryIndex() {
        newIndexBuf();
        newMemory();
    }

    public void put(int t, int prevT) {
        //比如对于1 2 3 4 5 6，间隔为2，会存 2 4 6
        if ((putCount++ % Const.INDEX_INTERVAL) == 0) {
            if (!indexBuf.hasRemaining()) {
                newIndexBuf();
            }
            //TODO:后面尝试换成直接put long
            //每隔INDEX_INTERVAL记录t（在indexBuf中记录了t就不会在memory中记录）
            indexBuf.putInt(t);

            if (!memory.hasRemaining()) {
                newMemory();
            }
            //以及记录下一个t所在memory的地址
            indexBuf.putInt((memory.putBitLength << 8) | memoryPos);
        } else {
            int diffT = t - prevT;
            if (!memory.put(diffT)) {
                newMemory();
                memory.put(diffT);
            }
        }
    }

    public int[] range(MemoryGetItem sItem, MemoryGetItem eItem) {
        //TODO 多线程并发，每个线程需要使用自己的memory对象
        if (sItem.memIndex >= memories.size()) {
            return new int[0];
        }
        //数据可能会存在多个块中
        int len = eItem.pos - sItem.pos;
        int[] res = new int[len];
        int i = 0;
        while (i < len) {
            Memory memory = memories.get(sItem.memIndex);
            memory.readBitPos= sItem.memPos;
            //需要读取的
            int end = sItem.memIndex == eItem.memIndex ? eItem.memPos : memory.putBitLength;
            while (memory.readBitPos < end) {
                res[i++] = VariableUtils.get(memory);
            }
            sItem.memIndex++;
            sItem.memPos = 0;
        }
        return res;
    }
    /**
     * 查找第一个大于val的数字位置
     */
    public MemoryGetItem upperBound(int val) {
        MemoryGetItem item = new MemoryGetItem();

        int indexPos = firstLess(val);
        if (indexPos >= 0) {
            //没有找到比val小的，返回第一个元素的位置

            int indexBufsPos = indexPos / Const.INDEX_BUFFER_SIZE;
            int indexBufPos = indexPos % Const.INDEX_BUFFER_SIZE;

            ByteBuffer bb = indexBufs.get(indexBufsPos);
            //得到这个位置的t值
            int lbVal = bb.getInt(indexBufPos);
            //得到这个位置的
            int nextValPosInfo = bb.getInt(indexBufPos + 4);
            //(memory.putBitLength << 8) | memoryPos;
            int nextIndex = nextValPosInfo & 0xff;
            int nextPos = nextValPosInfo >> 8;

            int eleCount = indexPos * Const.INDEX_INTERVAL + 1;

            Memory mem = memories.get(nextIndex);
            mem.readBitPos = nextPos;
            while (true) {
                lbVal += VariableUtils.get(memory);
                if (lbVal > val) {
                    //第一个大于val
                    item.pos = eleCount;
                    item.memIndex = nextIndex;
                    item.memPos = nextPos;
                    break;
                }

                eleCount++;
                if (mem.readBitPos >= mem.putBitLength) {
                    //这个内存块读到末尾了
                    if (++nextIndex > memories.size()) {
                        //所有内存块读完了，
                        item.pos = eleCount;
                        //只记录index，pos默认为0
                        item.memIndex = nextIndex;
                        break;
                    } else {
                        //跳转到下一个内存块
                        mem = memories.get(nextIndex);
                        mem.readBitPos = 0;
                    }
                }
                //记录当前这个数的起始位置
                nextPos = memory.readBitPos;
            }
        }
        return item;
    }


    /**
     * 查找第一个大于或等于val的数字位置
     */
    public MemoryGetItem lowerBound(int val) {
        MemoryGetItem item = new MemoryGetItem();

        int indexPos = firstLess(val);
        if (indexPos >= 0) {
            //没有找到比val小的，返回第一个元素的位置

            int indexBufsPos = indexPos / Const.INDEX_BUFFER_SIZE;
            int indexBufPos = indexPos % Const.INDEX_BUFFER_SIZE;

            ByteBuffer bb = indexBufs.get(indexBufsPos);
            //得到这个位置的t值
            int lbVal = bb.getInt(indexBufPos);
            //得到这个位置的
            int nextValPosInfo = bb.getInt(indexBufPos + 4);
            //(memory.putBitLength << 8) | memoryPos;
            int nextIndex = nextValPosInfo & 0xff;
            int nextPos = nextValPosInfo >> 8;

            int eleCount = indexPos * Const.INDEX_INTERVAL + 1;

            Memory mem = memories.get(nextIndex);
            mem.readBitPos = nextPos;
            while (true) {
                lbVal += VariableUtils.get(memory);
                if (lbVal >= val) {
                    item.pos = eleCount;
                    item.memIndex = nextIndex;
                    item.memPos = nextPos;
                    break;
                }

                eleCount++;
                if (mem.readBitPos >= mem.putBitLength) {
                    //这个内存块读到末尾了
                    if (++nextIndex > memories.size()) {
                        //所有内存块读完了，
                        item.pos = eleCount;
                        //只记录index，pos默认为0
                        item.memIndex = nextIndex;
                        break;
                    } else {
                        //跳转到下一个内存块
                        mem = memories.get(nextIndex);
                        mem.readBitPos = 0;
                    }
                }
                //记录当前这个数的起始位置
                nextPos = memory.readBitPos;
            }
        }
        return item;
    }

    /**
     * 查找第一个小于val的数字位置
     */
    private int firstLess(int val) {
        int low = 0, high = indexBufEleCount, mid, indexBufsPos, indexBufPos;

        //先找第一个大于等于val的位置，减1就是第一个小于val的位置
        while (low < high) {
            mid = low + (high - low) / 2;
            indexBufsPos = mid / Const.INDEX_BUFFER_SIZE;
            indexBufPos = mid % Const.INDEX_BUFFER_SIZE;

            long t = indexBufs.get(indexBufsPos).getInt(indexBufPos * Const.INDEX_ELE_SIZE);

            if (val > t) {
                low = mid + 1;
            }
            else {
                high = mid;
            }
        }
        return low - 1;
    }

    public void flush() {
        indexBufEleCount = (indexBufs.size() * Const.INDEX_BUFFER_SIZE - indexBufs.get(0).remaining()) / Const.INDEX_ELE_SIZE;
        Utils.print("MemoryIndex func=flush memoryIndexBuf=" + memoryIndexBufCounter.get() + " memoryBuf=" + memoryBufCounter.get());
    }

    private void newIndexBuf() {
        indexBuf = ByteBuffer.allocateDirect(Const.INDEX_BUFFER_SIZE);
        indexBufs.add(indexBuf);
        memoryIndexBufCounter.getAndIncrement();
    }

    private void newMemory() {
        memory = new Memory();
        memories.add(memory);
        ++memoryPos;
        memoryBufCounter.incrementAndGet();
    }
}
