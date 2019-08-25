package io.openmessaging;


import io.netty.util.concurrent.FastThreadLocal;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

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
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024);
        byteBuffer.limit(1024);
        while (byteBuffer.hasRemaining()) {
            System.out.println(byteBuffer.getLong());
        }
        test();
        int[] nums = new int[] {1, 2, 3};
        System.out.println(lowerBound(nums, 0, 3, 0));

        System.out.println(upperBound(nums, 0, 3, 0));

        VariableUtilsTest();

    }

    private static void test() {
        long[] t = new long[] {
            686, 569, 892, 167, 512, 107, 298, 999, 15, 5, 62, 557, 811, 292, 179, 747, 716, 285, 492, 621, 727, 921, 395, 621, 988, 849, 53, 13, 588, 719, 225, 224, 536, 886, 463, 110, 13, 152, 325, 402, 148, 570, 748, 442, 318, 45, 106, 99, 12, 948, 137, 238, 492, 269, 645, 378, 880, 175, 674, 375, 120, 854, 371, 44, 743, 781, 431, 134, 380, 240, 918, 782, 775, 893, 794, 382, 539, 319, 297, 704, 155, 575, 646, 274, 917, 359, 511, 777, 523, 620, 801, 137, 408, 880, 559, 839, 95, 240, 50, 460
        };

        ByteBuffer buf = ByteBuffer.allocateDirect(1024);
        Encoder encoder = new Encoder(buf);

        for (int i = 1; i < 100; i++) {
            encoder.encode((int) (t[i] - t[i - 1]));
        }
        encoder.flush();

        long tt[] = new long[100];
        tt[0] = t[0];
        Decoder decoder = new Decoder();
        for (int i = 1; i < 100; i++) {
            decoder.decode(buf, tt, 1, 0, 99);
        }
        System.out.println(Arrays.toString(tt));
        System.out.println();
    }

    private static void VariableUtilsTest() {
        int[] dest = new int[1];

        for (int i = 0; i < 1024 * 1024 * 10; i++) {
            byte[] data = new byte[64];
            VariableUtils.putUnsigned(ByteBuffer.wrap(data), 0, i);
            int unsigned = VariableUtils.getUnsigned(ByteBuffer.wrap(data), 0, dest, 0);
            if (dest[0] != i) {
                System.out.println(false);
            }
        }

        for (int i = -3615850; i < 3615850; i++) {
            byte[] data = new byte[50];
            VariableUtils.putSigned(ByteBuffer.wrap(data), 0, i);
            VariableUtils.getSigned(ByteBuffer.wrap(data), 0, dest, 0);
            if (dest[0] != i) {
                System.out.println(false);
            }
        }

        long startTime = System.currentTimeMillis();
        byte[] data = new byte[50];
        for (long i = 0L; i < 1024L * 1024 * 1024 * 2; i += 10) {
            for (int j = 0; j < 10; j++) {
                VariableUtils.putUnsigned(ByteBuffer.wrap(data), 0, j);
            }
        }
        System.out.println(System.currentTimeMillis() - startTime);

        System.out.println(new FastThreadLocal<String>() {
            @Override
            public String initialValue()
            {
                return "test";
            }
        }.get());
    }
}
