package io.openmessaging;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class UnsafeHolder {
    public static Unsafe unsafe;
    static {
        Field f = null;
        try {
            f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            unsafe = (Unsafe) f.get(null);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.exit(-1);
        }
    }
}
