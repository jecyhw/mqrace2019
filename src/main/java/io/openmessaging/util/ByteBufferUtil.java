package io.openmessaging.util;

import io.openmessaging.model.IntervalSum;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by yanghuiwei on 2019-09-01
 */
public class ByteBufferUtil {
    public static void readInBuf(long pos, ByteBuffer bb, FileChannel fc) {
        try {
            while (bb.hasRemaining()) {
                int read = fc.read(bb, pos);
                pos += read;
            }
        } catch (IOException e) {
            Utils.print(e.getMessage() + " method:readInBuf");
            System.exit(-1);
        }
    }

    public static void flush(ByteBuffer byteBuffer, FileChannel fc){
        byteBuffer.flip();
        try {
            while (byteBuffer.hasRemaining()){
                fc.write(byteBuffer);
            }
        } catch (IOException e) {
            Utils.print("func=flush " + e.getMessage());
            System.exit(-1);
        }
        byteBuffer.clear();
    }

    public static void sumChunkA(ByteBuffer as, int len, long aMin, long aMax, IntervalSum intervalSum) {
        long sum = 0;
        int count = 0;
        for (int i = 0; i < len; i++) {
            long a = as.getLong();
            if (aMin <= a && a <= aMax) {
                count++;
                sum += a;
            }
        }
        intervalSum.add(sum, count);
    }

    public static void inverseSumChunkA(ByteBuffer as, int len, long aMin, long aMax, IntervalSum intervalSum) {
        long sum = 0;
        int count = 0;
        for (int i = 0; i < len; i++) {
            long a = as.getLong();
            if (aMin <= a && a <= aMax) {
                count++;
                sum += a;
            }
        }
        intervalSum.remove(sum, count);
    }
}

