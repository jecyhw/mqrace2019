package io.openmessaging;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class UnsafeArray {
    private int unit = 4;
    private final static Unsafe unsafe;
    private int capacity;
    private int curIndex;
    private long address;

    static {
        unsafe = UnsafeHolder.unsafe;
    }

    /**
     *
     * @param capacity 数组总容量
     * @param unit 数组元素占用byte数
     */
    public UnsafeArray(int capacity, int unit) {
        this.capacity = capacity;
        address = unsafe.allocateMemory(capacity * unit);
        curIndex = 0;
        this.unit = unit;
    }

    public UnsafeArray(int capacity) {
        this(capacity, 4);
    }

    public final void set(int pos, int val){
        unsafe.putInt(address + pos * unit, val);
    }

    public final void add(int val){
        //为了性能考虑,这里不判断是否超容量，需要自己计算好使用容量
        unsafe.putInt(address + (curIndex++) * unit, val);
    }

    public final int get(int pos){
       return unsafe.getInt(address + pos * unit);
    }

    public final int size(){
        return curIndex;
    }

    public final int capacity(){
        return capacity;
    }

    public final int unit(){
        return unit;
    }
}
