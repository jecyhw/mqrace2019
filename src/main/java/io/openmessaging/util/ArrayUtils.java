package io.openmessaging.util;

import io.openmessaging.Const;

import java.nio.ByteBuffer;

/**
 * Created by yanghuiwei on 2019-08-28
 */
public class ArrayUtils {
    public static int findFirstGreatThanIndex(long[] arr, long val, int low, int high) {
        int mid;
        while(low < high){
            mid = low + (high - low) / 2;

            if(arr[mid] > val) {
                high = mid;
            }
            else {
                low = mid + 1;
            }
        }
        return low;
    }

    public static int findFirstGreatThanIndex(ByteBuffer buf, long val, int low, int high) {
        int mid;
        while(low < high){
            mid = low + (high - low) / 2;

            if(buf.getLong(mid * Const.LONG_BYTES) > val) {
                high = mid;
            }
            else {
                low = mid + 1;
            }
        }
        return low;
    }

    /**
     * 查找第一个小于val的数字位置，如果没有将返回PrimaryIndex的第一个位置
     */
    public static int findFirstLessThanIndex(long[] arr, long val, int low, int high) {
        int srcLow = low;
        int mid;
        //先找第一个大于等于val的位置，减1就是第一个小于val的位置
        while (low < high) {
            mid = low + (high - low) / 2;

            if (val > arr[mid]) {
                low = mid + 1;
            }
            else {
                high = mid;
            }
        }
        return low == srcLow ? low : low - 1;
    }

    public static int findFirstLessThanIndex(ByteBuffer buf, long val, int low, int high) {
        int srcLow = low;
        int mid;
        //先找第一个大于等于val的位置，减1就是第一个小于val的位置
        while (low < high) {
            mid = low + (high - low) / 2;

            if (val > buf.getLong(mid * Const.LONG_BYTES)) {
                low = mid + 1;
            }
            else {
                high = mid;
            }
        }
        return low == srcLow ? low : low - 1;
    }

}
