package io.openmessaging;

/**
 * Created by yanghuiwei on 2019-08-03
 */
public class MemoryGetItem {
    //下一个t所在的下标
    int nextMemIndex;
    //下一个memory数组所在的起始位置
    int nextMemPos;
    //在所有的t中属于第几个
    int pos;
    //数的值
    int t;

    public void set(int t, int pos, int memIndex, int memPos) {
        this.t = t;
        this.pos = pos;
        this.nextMemIndex = memIndex;
        this.nextMemPos = memPos;
    }
}
