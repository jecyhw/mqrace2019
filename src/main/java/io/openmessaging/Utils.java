package io.openmessaging;

import java.util.Date;

/**
 * Created by yanghuiwei on 2019-07-28
 */
public abstract class Utils {
    public static void print(String msg) {
        System.out.println(new Date().toString() + " " + msg);
    }

    public static String bytesToHex(byte[] hashInBytes) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < hashInBytes.length; i++) {
            sb.append(String.format("%02x", hashInBytes[i]));
        }
        return sb.toString();
    }

    public static void dPrint(Object obj) {
        System.out.println(obj);
    }
}
