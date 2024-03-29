package io.openmessaging;

import io.openmessaging.index.TAIndex;

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

    static int getMsgCount = 0;
    public static void updateMaxMsgNum(int msgNum) {
        synchronized (Monitor.class) {
            if (msgNum > getMaxMsgNum) {
                getMaxMsgNum = msgNum;
            }
            getMsgCount++;
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

        TAIndex.taIndex.log(sb);
//
//        int putCount = 0;
//        for (MessageFile messageFile : DefaultMessageStoreImpl.messageFiles) {
//            putCount += messageFile.putCount;
//        }
//
//
//        sb.append("putCount:").append(putCount).append(" INDEX_INTERVAL:").append(Const.INDEX_INTERVAL).append(",INDEX_ELE_LENGTH:").append(Const.INDEX_ELE_LENGTH).append("\n");
//        sb.append("put cost time:").append(getMsgStartTime - putStartTime)
//                .append(",get msg cost time:").append(getAvgTimes[0] - getMsgStartTime)
//                .append(",get avg cost time:").append(getAvgTimes[getAvgCounter.get()] - getAvgTimes[0])
//                .append(",get avg cost time1:").append(System.currentTimeMillis() - getAvgTimes[0]);
//
//        sb.append(",getAvgCounter:").append(getAvgCounter.get()).append(",getMaxMsgNum:").append(getMaxMsgNum)
//                .append(",getMsgCount:").append(getMsgCount).append(",").append("\n");
//
    }
}
