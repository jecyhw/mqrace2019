package io.openmessaging;

import static io.openmessaging.VariableUtils.get;
import static io.openmessaging.VariableUtils.put;

/**
 * Created by yanghuiwei on 2019-07-28
 */
public class Tester {
    private static int lowerBound(int[] a, int low, int high, int element){
        while(low < high){
            int middle = low + (high - low)/2;
            if(element > a[middle])
                low = middle + 1;
            else
                high = middle;
        }
        return low;
    }


    private static int upperBound(int[] a, int low, int high, int element){
        while(low < high){
            int middle = low + (high - low)/2;
            if(a[middle] > element)
                high = middle;
            else
                low = middle + 1;
        }
        return low;
    }

    public static void main(String[] args) {
        test();
        int[] nums = new int[] {1, 2, 3};
        System.out.println(lowerBound(nums, 0, 3, 0));

        System.out.println(upperBound(nums, 0, 3, 0));

        VariableUtilsTest();

    }

    private static void test() {
        int index = 1023, pos = 1024 * 1024;
        int t = (pos << 10) | index;

        System.out.println(t & 0x3ff);
        System.out.println(t >> 10);
    }

    private static void VariableUtilsTest() {
        MemoryRead memoryRead = new MemoryRead();
        for (int i = 0; i < 1024 * 1024 * 10; i++) {
            byte[] data = new byte[50];
            memoryRead.bitPos = 0;
            put(data, 0, i);
            if (get(data, memoryRead) != i) {
                System.out.println(false);
            }
        }

        long startTime = System.currentTimeMillis();
        byte[] data = new byte[50];
        for (long i = 0L; i < 1024L * 1024 * 1024 * 2; i += 10) {
            for (int j = 0; j < 10; j++) {
                put(data, 0, j);
            }
        }
        System.out.println(System.currentTimeMillis() - startTime);
    }
}
