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
    List<ByteBuffer> indexBufs = new ArrayList<>();
    //当前索引正在使用的内存
    ByteBuffer indexBuf;
    //indexBufs存的元素个数
    int indexBufEleCount = 0;

    //t的相对值存储的内存
    List<Memory> tMemories = new ArrayList<>();
    List<Memory> aMemories = new ArrayList<>();
    Memory tMemory;
    Memory aMemory;

    int aMin, aMax;
    long sum;

    public MemoryIndex() {
        newIndexBuf();
        newTMemory();
        newAMemory();
    }

    public void put(int t, int prevT, int a, int prevA) {
        //比如对于1 2 3 4 5 6，间隔为2，会存 1 3 5
        if (putCount % Const.INDEX_INTERVAL == 0) {
            if (!indexBuf.hasRemaining()) {
                newIndexBuf();
            }

            //TODO:后面尝试换成直接put long
            //每隔INDEX_INTERVAL记录t（在indexBuf中记录了t就不会在memory中记录）
            indexBuf.putInt(t);
            //下一个t的起始位置，先写在哪个块中，再写块的便宜位置
            if (tMemory.hasRemaining()) {
                indexBuf.putShort((short)(tMemories.size() - 1));
                indexBuf.putInt(tMemory.putBitLength);
            } else {
                indexBuf.putShort((short) tMemories.size());
                indexBuf.putInt(0);
            }

            indexBuf.putInt(a);
            if (aMemory.hasRemaining()) {
                indexBuf.putShort((short)(aMemories.size() - 1));
                indexBuf.putInt(aMemory.putBitLength);
            } else {
                indexBuf.putShort((short) aMemories.size());
                indexBuf.putInt(0);
            }


            indexBuf.putInt(aMin);
            indexBuf.putInt(aMax);
            indexBuf.putLong(sum);


            aMin = Integer.MAX_VALUE;
            aMax = Integer.MIN_VALUE;
            sum = 0;
        } else {
            int diffT = t - prevT;
            if (!tMemory.putUnsigned(diffT)) {
                newTMemory();
                tMemory.putUnsigned(diffT);
            }

            int diffA = a - prevA;
            if (!aMemory.putSigned(diffA)) {
                newAMemory();
                aMemory.putSigned(diffA);
            }

            if (aMin > a) {
                aMin = a;
            }
            if (aMax < a) {
                aMax = a;
            }
            sum += a;
        }
        putCount++;
    }

    public int[] rangeT(MemoryGetItem sItem, MemoryGetItem eItem, MemoryRead memoryRead) {
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

        int nextMemIndex = sItem.tNextMemIndex;
        memoryRead.tBitPos = sItem.tNextMemPos;

        while (i < len) {
            if ((curPos % Const.INDEX_INTERVAL) == 0) {
                //从索引内存中读
                int indexPos = curPos / Const.INDEX_INTERVAL;

                int indexBufsPos = indexPos / Const.INDEX_ELE_LENGTH;
                int indexBufPos = indexPos % Const.INDEX_ELE_LENGTH;

                ByteBuffer tIndexBuf = indexBufs.get(indexBufsPos).duplicate();
                //得到索引内存中这个位置的t值
                int bufOffset = indexBufPos * Const.INDEX_ELE_SIZE;
                curT = tIndexBuf.getInt(bufOffset);
                //得到这个t的下一个t在内存块的位置
                nextMemIndex = tIndexBuf.getShort(bufOffset + 4);
                //得到这个t的下一个t在内存块中的偏移位置
                memoryRead.tBitPos = tIndexBuf.getInt(bufOffset + 6);
                //得到这个位置的t值

                res[i] = curT;
            } else {
                //从变长编码内存中读
                Memory mem = tMemories.get(nextMemIndex);
                curT += VariableUtils.getUnsigned(mem.data, memoryRead);

                res[i] = curT;

                if (memoryRead.tBitPos >= mem.putBitLength) {
                    nextMemIndex++;
                    memoryRead.tBitPos = 0;
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
//        System.out.println(Thread.currentThread().getName() + " bound-----------------------------------------");
        int curPos = firstLess(val) * Const.INDEX_INTERVAL;
        int curT = 0, tNextMemIndex = 0, curA = 0, aNextMemIndex = 0;
        int tMemorySize = tMemories.size(), aMemorySize = aMemories.size();

        if (curPos >= 0) {
            while (true) {
                if ((curPos % Const.INDEX_INTERVAL) == 0) {
                    //数据在索引内存中
                    if (curPos == putCount) {
                        //总共putCount，数据下标从0开始，curPos到了putCount，说明没有数据了
                        item.setTInfo(putCount, 0, tMemorySize, 0);
                        item.setAInfo(0, aMemorySize, 0);
                        return;
                    }
                    //从索引内存中读
                    int indexPos = curPos / Const.INDEX_INTERVAL;

                    int indexBufsPos = indexPos / Const.INDEX_ELE_LENGTH;
                    int indexBufPos = indexPos % Const.INDEX_ELE_LENGTH;

                    ByteBuffer tIndexBuf = indexBufs.get(indexBufsPos).duplicate();
                    //得到索引内存中这个位置的t值
                    int bufOffset = indexBufPos * Const.INDEX_ELE_SIZE;
                    curT = tIndexBuf.getInt(bufOffset);
                    //得到这个t的下一个t在内存块的位置
                    tNextMemIndex = tIndexBuf.getShort(bufOffset + 4);
                    //得到这个t的下一个t在内存块中的偏移位置
                    memoryRead.tBitPos = tIndexBuf.getInt(bufOffset + 6);

                    curA = tIndexBuf.getInt(bufOffset + 10);
                    aNextMemIndex = tIndexBuf.getShort(bufOffset + 14);
                    memoryRead.aBitPos = tIndexBuf.getInt(bufOffset + 16);

                    //检查当前在索引内存中的t是不是符合条件
                    if (check(curT, curPos, tNextMemIndex, memoryRead.tBitPos, val, item, isLt)) {
                        item.setAInfo(curA, aNextMemIndex, memoryRead.aBitPos);
                        return;
                    }

                    if (tNextMemIndex >= tMemorySize) {
                        //说明索引内存中的t是最后一个元素，此时nextMemIndex会等于memories.size()，并且memoryRead.bitPos=0
//                            System.out.println(Thread.currentThread().getName() + " 2.pos:" + curPos + " putC:" + putCount + " ct:" + curT + " t:" + val + " neInd:" + tNextMemIndex + " nxBitPos:" + memoryRead.bitPos + " memoryPos:" + memoryPos);
                        item.setTInfo(curPos + 1, 0, tMemorySize, 0);
                        item.setAInfo(0, aMemorySize, 0);
                        return;
                    }
                } else {
                    //数据不在索引内存中，从变长编码内存中读
                    Memory tMem = tMemories.get(tNextMemIndex);
                    Memory aMem = aMemories.get(aNextMemIndex);

                    curT += VariableUtils.getUnsigned(tMem.data, memoryRead);
                    curA += VariableUtils.getSigned(aMem.data, memoryRead);

//                    System.out.println(Thread.currentThread().getName() + " 3.pos:" + curPos + " ct:" + curT + " t:" + val + " neInd:" + tNextMemIndex + " nxBitPos:" + memoryRead.bitPos + " memoryPos:" + memoryPos);
                    if (memoryRead.tBitPos >= tMem.putBitLength) {
                        //这个t刚好在内存块的最后一个位置,更新下一个t到下一个块中的起始位置信息
                        tNextMemIndex++;
                        memoryRead.tBitPos = 0;
                    }

                    if (memoryRead.aBitPos >= aMem.putBitLength) {
                        aNextMemIndex++;
                        memoryRead.aBitPos = 0;
                    }

                    //校验变长编码内存中的t是不是符合条件
                    if (check(curT, curPos, tNextMemIndex, memoryRead.tBitPos, val, item, isLt)) {
                        item.setAInfo(curA, aNextMemIndex, memoryRead.aBitPos);
                        return;
                    }
                    if (tNextMemIndex >= tMemorySize) {
                        //所有内存块读完了
//                                System.out.println(Thread.currentThread().getName() + " 4.pos:" + curPos + " putC:" + putCount + " ct:" + curT + " t:" + val + " neInd:" + tNextMemIndex + " nxBitPos:" + memoryRead.bitPos + " memoryPos:" + memoryPos);
                        item.setTInfo(curPos + 1, 0, tMemorySize, 0);
                        item.setAInfo(0, aMemorySize, 0);
                        return;
                    }
                }

                curPos++;
            }
        } else {
            //pos表示0是第一个元素
            item.setTInfo(0, 0, 0, 0);
            item.setAInfo(0, 0, 0);
        }
    }

    private boolean check(int candidateT, int candidateTCount, int nextCandidateTIndex, int nextCandidateTPos, int destT, MemoryGetItem item, boolean isLt) {
        if (isLt) {
            if (candidateT > destT) {
                item.setTInfo(candidateTCount, candidateT, nextCandidateTIndex, nextCandidateTPos);
                return true;
            }
        } else {
            if (candidateT >= destT) {
                item.setTInfo(candidateTCount, candidateT, nextCandidateTIndex, nextCandidateTPos);
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
        //TODO 需要注意最后一块的aMin,aMax,sum在内存中，没有放到buffer中
        indexBufEleCount = (indexBufs.size() * Const.INDEX_BUFFER_SIZE - indexBufs.get(indexBufs.size() - 1).remaining()) / Const.INDEX_ELE_SIZE;
        Utils.print("MemoryIndex func=flush indexBuf=" + indexBufCounter.get() + " tBuf=" + tBufCounter.get() + " aBuf:" + aBufCounter.get());
    }

    private void newIndexBuf() {
        indexBuf = ByteBuffer.allocateDirect(Const.INDEX_BUFFER_SIZE);
        indexBufs.add(indexBuf);
        indexBufCounter.getAndIncrement();
    }

    private void newTMemory() {
        tMemory = new Memory();
        tMemories.add(tMemory);
        tBufCounter.incrementAndGet();
    }

    private void newAMemory() {
        aMemory = new Memory();
        aMemories.add(aMemory);
        aBufCounter.incrementAndGet();
    }

    public void rangeA(MemoryGetItem sItem, MemoryGetItem eItem, MemoryRead memoryRead, int[] as) {
        int curPos = sItem.pos;
        //数据可能会存在多个块中
        int len = eItem.pos - curPos;

        int i = 0;
        int curT = 0;

        if (curPos != 0) {
            as[i++] = curT = sItem.a;
            curPos++;
        }

        int nextMemIndex = sItem.aNextMemIndex;
        memoryRead.aBitPos = sItem.aNextMemPos;

        while (i < len) {
            if ((curPos % Const.INDEX_INTERVAL) == 0) {
                //从索引内存中读
                int indexPos = curPos / Const.INDEX_INTERVAL;

                int indexBufsPos = indexPos / Const.INDEX_ELE_LENGTH;
                int indexBufPos = indexPos % Const.INDEX_ELE_LENGTH;

                ByteBuffer tIndexBuf = indexBufs.get(indexBufsPos).duplicate();
                //得到索引内存中这个位置的t值
                int bufOffset = indexBufPos * Const.INDEX_ELE_SIZE;
                curT = tIndexBuf.getInt(bufOffset + 10);
                //得到这个t的下一个t在内存块的位置
                nextMemIndex = tIndexBuf.getShort(bufOffset + 14);
                //得到这个t的下一个t在内存块中的偏移位置
                memoryRead.aBitPos = tIndexBuf.getInt(bufOffset + 16);
                //得到这个位置的t值

                as[i] = curT;
            } else {
                //从变长编码内存中读
                Memory mem = aMemories.get(nextMemIndex);
                curT += VariableUtils.getSigned(mem.data, memoryRead);

                as[i] = curT;

                if (memoryRead.aBitPos >= mem.putBitLength) {
                    nextMemIndex++;
                    memoryRead.aBitPos = 0;
                }
            }
            i++;
            curPos++;
        }
    }
}
