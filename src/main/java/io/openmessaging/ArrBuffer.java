package io.openmessaging;

public class ArrBuffer {
    private int[] as;
    private int[] ts;

    public ArrBuffer() {
        as = new int[80*10000+100];
//        ts = new int[80*10000+100];
    }

    public int[] getAs() {
        return as;
    }

    public int[] getTs() {
        return ts;
    }
}