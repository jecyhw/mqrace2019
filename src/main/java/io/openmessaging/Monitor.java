package io.openmessaging;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yanghuiwei on 2019-08-08
 */
public class Monitor {
    public static volatile int getMaxMsgNum = 0;
    public static volatile long putStartTime;

    public static volatile long getMsgStartTime;

    private static volatile AtomicInteger getAvgCounter = new AtomicInteger(-1);
    public static volatile long[] getAvgTimes = new long[Const.GET_AVG_COUNT];


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

    public static void getAvgStat() {
        getAvgTimes[getAvgCounter.incrementAndGet()] = System.currentTimeMillis();
    }

    public static void log() {
        StringBuilder sb = new StringBuilder();
        sb.append("INDEX_INTERVAL:").append(Const.INDEX_INTERVAL).append(",INDEX_ELE_LENGTH:").append(Const.INDEX_ELE_LENGTH).append("\n");
        sb.append("put cost time:").append(getMsgStartTime - putStartTime)
                .append(",get msg cost time:").append(getAvgTimes[0] - getMsgStartTime)
                .append(",get avg cost time:").append(getAvgTimes[getAvgCounter.get()] - getAvgTimes[0])
                .append(",get avg cost time1:").append(System.currentTimeMillis() - getAvgTimes[0]);

        sb.append(",getAvgCounter:").append(getAvgCounter.get()).append(",getMaxMsgNum:").append(getMaxMsgNum).append("\n");

        Utils.print(sb.toString());

        if (Const.PRINT_ERR) {
            System.err.println("func=shutdownHook stop");
        }
    }
}
