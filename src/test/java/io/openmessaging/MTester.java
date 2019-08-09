package io.openmessaging;

public class MTester {
    public static void main(String[] args) {
        int size =1024 * 1024 * 130;
        UnsafeMemory unsafeMemory = new UnsafeMemory(size);
        for (int i = 0; i < size; i++){
            unsafeMemory.put((byte) 0xff);
        }

        for (int i = 0; i < size; i++){
            unsafeMemory.put(i, (byte) 0xff);
        }

        for (long i = unsafeMemory.size()-1; i >= 0; i--){
            byte b = unsafeMemory.get(i);
        }
    }
}
