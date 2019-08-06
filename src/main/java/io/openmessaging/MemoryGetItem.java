package io.openmessaging;

/**
 * Created by yanghuiwei on 2019-08-03
 */
public class MemoryGetItem {
    //在所有的t中属于第几个
    int pos;

    //下一个t所在的下标
    int tNextMemIndex;
    //下一个memory数组所在的起始位置
    int tNextMemPos;
    //数的值
    int t;

    //下一个t所在的下标
    int aNextMemIndex;
    //下一个memory数组所在的起始位置
    int aNextMemPos;
    //数的值
    int a;

    public void setTInfo(int pos, int t, int tMemIndex, int tMemPos) {
        this.pos = pos;
        this.t = t;
        this.tNextMemIndex = tMemIndex;
        this.tNextMemPos = tMemPos;
    }

    public void setAInfo(int a, int aMemIndex, int aMemPos) {
        this.a = a;
        this.aNextMemIndex = aMemIndex;
        this.aNextMemPos = aMemPos;
    }
}
