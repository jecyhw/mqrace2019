package io.openmessaging;


import io.openmessaging.codec.ADecoder;
import io.openmessaging.codec.AEncoder;
import io.openmessaging.codec.TDecoder;
import io.openmessaging.codec.TEncoder;

import java.nio.ByteBuffer;
import java.util.Arrays;

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
        long a = 0x3ffffffffffffff3L;
        System.out.println(a);
        int  c = (int)a;
        System.out.println(c);
        System.out.println((c & 0xffffffffL));

        testA();
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


    }

    private static void testA() {
        System.out.println(Integer.toBinaryString((1 << 31) - 1));
        long[] t = new long[] {

                3931612145552529380L, 4163447499782559055L, 6467434659834263721L, 5533586936800809475L, 6292235149651601446L,
                5406343008289214419L, 8663209456284807484L, 4985868773886143259L, 6259814026459997774L, 1214603110414461106L,
                7330317154667833251L, 143290442028709725L, 6742744982751692109L
        };


        System.out.println(Arrays.toString(t));

        ByteBuffer buf = ByteBuffer.allocateDirect(1024);
        AEncoder encoder = new AEncoder(buf);

        for (int i = 1; i < t.length; i++) {
            encoder.encode(t[i] - t[i - 1]);
        }
        encoder.flushAndClear();

        long tt[] = new long[100];
        tt[0] = t[0];
        ADecoder decoder = new ADecoder();
        decoder.reset(buf, 0);
        decoder.decode(tt, 1, t.length - 1);
        System.out.println(Arrays.toString(tt));


        for (int i = 1; i < t.length; i++) {
            if (tt[i] != t[i]) {
                System.out.println(i);
                System.exit(0);
            }
        }

        System.out.println();
    }

    private static void test() {
        long[] t = new long[] {
            686, 569, 892, 167, 512, 107, 298, 999, 15, 5, 62, 557, 811, 292, 179, 747, 716, 285, 492, 621, 727, 921, 395, 621, 988, 849, 53, 13, 588, 719, 225, 224, 536, 886, 463, 110, 13, 152, 325, 402, 148, 570, 748, 442, 318, 45, 106, 99, 12, 948, 137, 238, 492, 269, 645, 378, 880, 175, 674, 375, 120, 854, 371, 44, 743, 781, 431, 134, 380, 240, 918, 782, 775, 893, 794, 382, 539, 319, 297, 704, 155, 575, 646, 274, 917, 359, 511, 777, 523, 620, 801, 137, 408, 880, 559, 839, 95, 240, 50, 460
        };

        ByteBuffer buf = ByteBuffer.allocateDirect(1024);
        TEncoder encoder = new TEncoder(buf);

        for (int i = 1; i < 100; i++) {
            encoder.encode((int) (t[i] - t[i - 1]));
        }
        encoder.flushAndClear();

        long tt[] = new long[100];
        tt[0] = t[0];
        TDecoder decoder = new TDecoder();
        for (int i = 1; i < 100; i++) {
            decoder.decode(buf, tt, 1, 0, 99);
        }
        System.out.println(Arrays.toString(tt));
        System.out.println();
    }

}
