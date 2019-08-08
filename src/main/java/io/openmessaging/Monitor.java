package io.openmessaging;

/**
 * Created by yanghuiwei on 2019-08-08
 */
public class Monitor {
    public static volatile int getMaxMsgNum = 0;
    public static volatile long putStartTime;

    public static volatile long getMsgStartTime;

    public static volatile long getAvgStartTime;


    public static void updateMaxMsgNum(int msgNum) {
        synchronized (Monitor.class) {
            if (msgNum > getMaxMsgNum) {
                getMaxMsgNum = msgNum;
            }
        }
    }

    public static void putStart() {
        putStartTime = System.currentTimeMillis();
    }

    public static void getMsgStart() {
        getMsgStartTime = System.currentTimeMillis();
    }

    public static void getAvgStart() {
        getAvgStartTime = System.currentTimeMillis();
    }

    public static void log() {
        StringBuilder sb = new StringBuilder();
        sb.append("put cost time:").append(getMsgStartTime - putStartTime)
                .append(",get msg cost time:").append(getAvgStartTime - getMsgStartTime)
                .append(",get avg cost time:").append(System.currentTimeMillis() - getAvgStartTime);

        sb.append("getMaxMsgNum:").append(getMaxMsgNum).append("\n");


        Utils.print(sb.toString());
    }
}
