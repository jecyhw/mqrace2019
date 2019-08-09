package io.openmessaging;

import sun.misc.Unsafe;

public class UnsafeMemory {
    private  final static Unsafe unsafe;
    private long capacity;
    private long curIndex;
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

    public final void put(long pos, byte val){
        unsafe.putByte(address + pos, val);
    }

    public final byte get(long pos){
        return unsafe.getByte(address + pos);
    }

    public static UnsafeMemory wrap(byte[] bytes){
        UnsafeMemory memory = new UnsafeMemory(bytes.length);
        for (int i = bytes.length-1; i >= 0; i--){
            memory.put(i, bytes[i]);
        }
        return  memory;
    }

    public final long capacity(){
        return capacity;
    }

    public final long size(){
        return curIndex;
    }
}
