package io.openmessaging;

import sun.misc.Unsafe;

public class UnsafeMemory {
    private final static Unsafe unsafe;
    private int capacity;
    private int curIndex;
    private long address;

    static {
        unsafe = UnsafeHolder.unsafe;
    }

    public UnsafeMemory(long capacity) {
        address = unsafe.allocateMemory(capacity);
        curIndex = 0;
    }

    public final void put(byte val){
        unsafe.putByte(address + curIndex++, val);
    }

    public final void put(int pos, byte val){
        unsafe.putByte(address + pos, val);
    }

    public final byte get(int pos){
        return unsafe.getByte(address + pos);
    }

    public static UnsafeMemory wrap(byte[] bytes){
        UnsafeMemory memory = new UnsafeMemory(bytes.length);
        for (int i = bytes.length-1; i >= 0; i--){
            memory.put(i, bytes[i]);
        }
        return  memory;
    }

    public final int capacity(){
        return capacity;
    }

    public final int size(){
        return curIndex;
    }

    public final void clear(){
        curIndex = 0;
    }
}
