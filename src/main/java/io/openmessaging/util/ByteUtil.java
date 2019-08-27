package io.openmessaging.util;

/**
 * Created by yanghuiwei on 2019-08-27
 */

public class ByteUtil {
    public static void putShort(byte b[], int s, int index) {
        b[index] = (byte) (s >> 8);
        b[index + 1] = (byte) (s);
    }

    public static int getShort(byte[] b, int index) {
        return (((b[index] << 8) | b[index + 1] & 0xff));
    }


    public static void putInt(byte[] bb, int x, int index) {
        bb[index] = (byte) (x >> 24);
        bb[index + 1] = (byte) (x >> 16);
        bb[index + 2] = (byte) (x >> 8);
        bb[index + 3] = (byte) (x);
    }

    public static int getInt(byte[] bb, int index) {
        return ((((bb[index] & 0xff) << 24)
                | ((bb[index + 1] & 0xff) << 16)
                | ((bb[index + 2] & 0xff) << 8) | ((bb[index + 3] & 0xff))));
    }


    public static void putLong(byte[] bb, long x, int index) {
        bb[index] = (byte) (x >> 56);
        bb[index + 1] = (byte) (x >> 48);
        bb[index + 2] = (byte) (x >> 40);
        bb[index + 3] = (byte) (x >> 32);
        bb[index + 4] = (byte) (x >> 24);
        bb[index + 5] = (byte) (x >> 16);
        bb[index + 6] = (byte) (x >> 8);
        bb[index + 7] = (byte) (x);
    }

    public static long getLong(byte[] bb, int index) {
        return ((((long) bb[index] & 0xff) << 56)
                | (((long) bb[index + 1] & 0xff) << 48)
                | (((long) bb[index + 2] & 0xff) << 40)
                | (((long) bb[index + 3] & 0xff) << 32)
                | (((long) bb[index + 4] & 0xff) << 24)
                | (((long) bb[index + 5] & 0xff) << 16)
                | (((long) bb[index + 6] & 0xff) << 8) | (((long) bb[index + 7] & 0xff)));
    }
}
