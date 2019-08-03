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
        //比如对于1 2 3 4 5 6，间隔为2，会存 1 3 5
        if ((putCount++ % Const.INDEX_INTERVAL) == 0) {
            if (!indexBuf.hasRemaining()) {
                newIndexBuf();
            }
            //TODO:后面尝试换成直接put long
            //每隔INDEX_INTERVAL记录t（在indexBuf中记录了t就不会在memory中记录）
            indexBuf.putInt(t);

            if (memory.hasRemaining()) {
                indexBuf.putInt((memory.putBitLength << Const.INDEX_BUFFER_BIT_LENGTH) | memoryPos);
            } else {
                indexBuf.putInt(memoryPos + 1);
            }
            //以及记录下一个t所在memory的地址，目前10个字节表示长度，注意这里有坑，需要合理计算
        } else {
            int diffT = t - prevT;
            if (!memory.put(diffT)) {
                newMemory();
                memory.put(diffT);
            }
        }
    }

    public int[] range(MemoryGetItem sItem, MemoryGetItem eItem, MemoryRead memoryRead) {
        if (sItem.nextMemIndex > memoryPos) {
            return null;
        }

        int curPos = sItem.pos;
        //数据可能会存在多个块中
        int len = eItem.pos - curPos;
        int[] res = new int[len];
        int i = 0;
        int curT = 0;

        if (curPos != 0) {
            res[i++] = curT = sItem.t;
            curPos++;
        }

        int nextMemIndex = sItem.nextMemIndex;
        memoryRead.bitPos = sItem.nextMemPos;



        while (i < len) {
            if ((curPos % Const.INDEX_INTERVAL) == 0) {
                //从索引内存中读
                int indexPos = curPos / Const.INDEX_INTERVAL;

                int indexBufsPos = indexPos / Const.INDEX_ELE_LENGTH;
                int indexBufPos = indexPos % Const.INDEX_ELE_LENGTH;

                ByteBuffer tIndexBuf = indexBufs.get(indexBufsPos).duplicate();
                //得到这个位置的t值
                curT = tIndexBuf.getInt(indexBufPos * Const.INDEX_ELE_SIZE);
                //得到这个t的下一个t存储的位置
                int nextValPosInfo = tIndexBuf.getInt(indexBufPos * Const.INDEX_ELE_SIZE + 4);
                //(memory.putBitLength << 8) | memoryPos;
                nextMemIndex = nextValPosInfo & Const.INDEX_BUFFER_BIT;
                memoryRead.bitPos = nextValPosInfo >> Const.INDEX_BUFFER_BIT_LENGTH;

                res[i] = curT;
            } else {
                //从变长编码内存中读
                Memory mem = memories.get(nextMemIndex);
                curT += VariableUtils.get(mem.data, memoryRead);

                res[i] = curT;

                if (memoryRead.bitPos >= mem.putBitLength) {
                    nextMemIndex++;
                    memoryRead.bitPos = 0;
                }
            }
            i++;
            curPos++;
        }
        return res;
    }

    /**
     * 查找第一个大于或等于val的数字位置
     */
    public void lowerBound(int val, MemoryRead memoryRead, MemoryGetItem item) {
        bound(val, memoryRead, item, false);

    }

    /**
     * 查找第一个大于val的数字位置
     */
    public void upperBound(int val, MemoryRead memoryRead, MemoryGetItem item) {
        bound(val, memoryRead, item, true);
    }

    private void bound(int val, MemoryRead memoryRead, MemoryGetItem item, boolean isLt) {
        int indexPos = firstLess(val);
        int candidateTPos = indexPos * Const.INDEX_INTERVAL;
        if (indexPos >= 0) {
            while (true) {
                int indexBufsPos = indexPos / Const.INDEX_ELE_LENGTH;
                int indexBufPos = indexPos % Const.INDEX_ELE_LENGTH;

                ByteBuffer tIndexBuf = indexBufs.get(indexBufsPos).duplicate();
                //得到这个位置的t值
                int candidateT = tIndexBuf.getInt(indexBufPos * Const.INDEX_ELE_SIZE);
                //得到这个t的下一个t存储的位置
                int nextValPosInfo = tIndexBuf.getInt(indexBufPos * Const.INDEX_ELE_SIZE + 4);
                //(memory.putBitLength << 8) | memoryPos;
                int nextCandidateTMemIndex = nextValPosInfo & Const.INDEX_BUFFER_BIT;
                int nextCandidateTMemPos = nextValPosInfo >> Const.INDEX_BUFFER_BIT_LENGTH;

                if (nextCandidateTMemIndex > memoryPos) {
                    item.set(0, putCount, nextCandidateTMemIndex, 0);
                    return;
                }

                if (check(candidateT, candidateTPos, nextCandidateTMemIndex, nextCandidateTMemPos, val, item, isLt)) {
                    return;
                }

                memoryRead.bitPos = nextCandidateTMemPos;
                Memory mem = memories.get(nextCandidateTMemIndex);
                byte[] data = mem.data;

                candidateTPos++;
                for (int i = 1; i < Const.INDEX_INTERVAL; i++) {
                    candidateT += VariableUtils.get(data, memoryRead);
                    if (memoryRead.bitPos >= mem.putBitLength) {
                        //这个内存块读到末尾了
                        if (++nextCandidateTMemIndex > memoryPos) {
                            //所有内存块读完了
                            item.set(0, putCount, nextCandidateTMemIndex, 0);
                            return;
                        } else {
                            //跳转到下一个内存块
                            mem = memories.get(nextCandidateTMemIndex);
                            memoryRead.bitPos = 0;
                        }
                    }
                    //必须要在 memoryRead.bitPos >= mem.putBitLength 判断之后
                    if (check(candidateT, candidateTPos, nextCandidateTMemIndex, memoryRead.bitPos, val, item, isLt)) {
                        return;
                    }
                    candidateTPos++;
                }
                indexPos++;
            }
        } else {
            //pos表示0是第一个元素
            item.set(0, 0, 0, 0);
        }
    }

    private boolean check(int candidateT, int candidateTCount, int nextCandidateTIndex, int nextCandidateTPos, int destT, MemoryGetItem item, boolean isLt) {
        if (isLt) {
            if (candidateT > destT) {
                item.set(candidateT, candidateTCount, nextCandidateTIndex, nextCandidateTPos);
                return true;
            }
        } else {
            if (candidateT >= destT) {
                item.set(candidateT, candidateTCount, nextCandidateTIndex, nextCandidateTPos);
                return true;
            }
        }
        return false;
    }

    /**
     * 查找第一个小于val的数字位置
     */
    private int firstLess(int val) {
        int low = 0, high = indexBufEleCount, mid, indexBufsPos, indexBufPos;

        //先找第一个大于等于val的位置，减1就是第一个小于val的位置
        while (low < high) {
            mid = low + (high - low) / 2;
            indexBufsPos = mid / Const.INDEX_ELE_LENGTH;
            indexBufPos = mid % Const.INDEX_ELE_LENGTH;

            long t = indexBufs.get(indexBufsPos).duplicate().getInt(indexBufPos * Const.INDEX_ELE_SIZE);

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
        indexBufEleCount = (indexBufs.size() * Const.INDEX_BUFFER_SIZE - indexBufs.get(indexBufs.size() - 1).remaining()) / Const.INDEX_ELE_SIZE;
        Utils.print("MemoryIndex func=flush memoryIndexBuf=" + memoryIndexBufCounter.get() + " memoryBuf=" + memoryBufCounter.get());
    }

    private void newIndexBuf() {
        indexBuf = ByteBuffer.allocateDirect(Const.INDEX_BUFFER_SIZE);
        indexBufs.add(indexBuf);
        memoryIndexBufCounter.getAndIncrement();

        Utils.print("func=newIndexBuf memoryIndexBufCounter=" + memoryIndexBufCounter.get());
    }

    private void newMemory() {
        memory = new Memory();
        memories.add(memory);
        ++memoryPos;
        memoryBufCounter.incrementAndGet();

        Utils.print("func=newMemory memoryBufCounter=" + memoryBufCounter.get() + " putCount=" + putCount);
    }
}
