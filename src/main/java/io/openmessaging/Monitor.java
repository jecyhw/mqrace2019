package io.openmessaging;

import io.openmessaging.util.Utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yanghuiwei on 2019-08-08
 */
public class Monitor {

    public static long STAGE_TIME = System.currentTimeMillis();
    private static int TIMEOUT = 420;
    public static String PID;

    public static StringBuilder stageBuilder = new StringBuilder();
    public static StringBuilder gcBuilder = new StringBuilder();
    public static StringBuilder iostatForDiskBuilder = new StringBuilder();
    public static StringBuilder iostatForCpuBuilder = new StringBuilder();
    public static StringBuilder freeBuilder = new StringBuilder();

    public static long firstPid = -1L;
    private static long lastT = -1;
    private static int uniqueTNum = 0;
    private static int tMaxInterval = 0;

    public static volatile int getMaxMsgNum = 0;
    public static volatile long putStartTime;

    public static volatile long getMsgStartTime;

    private static volatile AtomicInteger getAvgCounter = new AtomicInteger(-1);
    public static volatile long[] getAvgTimes = new long[Const.GET_AVG_COUNT];

    public static void init(){
        String name = ManagementFactory.getRuntimeMXBean().getName();
        System.out.println(name);
        PID = name.split("@")[0];

        final long timeout = System.currentTimeMillis()+1000*TIMEOUT;
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                while(true){
                    if (System.currentTimeMillis() > timeout){
                        System.out.println("[error] timeout---------------------");
                        System.exit(-1);
                    }

                    work();

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        System.out.println(e.getMessage());
                        System.exit(-1);
                    }
                }
            }
        });

        thread.setDaemon(true);
        thread.start();
    }

    private static void work(){
        try {
            gc();
//            iostatForDisk();
//            free();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.exit(-1);
        }
    }

    private static long[] scores = new long[3];
    public static void mark(int stage){
        switch (stage){
            case 0:
                stageBuilder.append("[stage cost] init time:" + (System.currentTimeMillis() - STAGE_TIME) + "ms date:" + new Date() + " time:" +System.currentTimeMillis());
                break;
            case 1:
            case 2:
            case 3:
                scores[stage-1] = System.currentTimeMillis() - STAGE_TIME;
                stageBuilder.append("[stage cost] stage-" + stage + " time:" + (System.currentTimeMillis() - STAGE_TIME) + "ms date:" + new Date() + " time:" +System.currentTimeMillis());
                break;
        }

        stageBuilder.append(System.lineSeparator());
        STAGE_TIME = System.currentTimeMillis();
        gcBuilder.append("stage "+stage + " ------------------------------------------------------------------------------------------------" + System.lineSeparator());
        iostatForDiskBuilder.append("stage "+stage + " ------------------------------------------------------------------------------------------------" + System.lineSeparator());
        iostatForCpuBuilder.append("stage "+stage + " ------------------------------------------------------------------------------------------------" + System.lineSeparator());
        freeBuilder.append("stage "+stage + " ------------------------------------------------------------------------------------------------" + System.lineSeparator());
    }

    private static volatile boolean monitorGCFirst = true;
    private static void gc() throws IOException {
        BufferedReader bufferedReaderGc = new BufferedReader(new InputStreamReader(Runtime.getRuntime().exec("jstat -gc "+PID).getInputStream()));
        if(monitorGCFirst){
            String str = bufferedReaderGc.readLine();
            gcBuilder.append(str + "    timestamp");
            gcBuilder.append(System.lineSeparator());
            monitorGCFirst = false;
        }else{
            bufferedReaderGc.readLine();
        }
        String str = bufferedReaderGc.readLine();
        gcBuilder.append(str + "    " + System.currentTimeMillis());
        gcBuilder.append(System.lineSeparator());
    }

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
        sb.append("INDEX_INTERVAL:").append(Const.INDEX_INTERVAL).append(",INDEX_ELE_LENGTH:").append(Const.INDEX_ELE_LENGTH).append("\n");
        sb.append("put cost time:").append(getMsgStartTime - putStartTime)
                .append(",get msg cost time:").append(getAvgTimes[0] - getMsgStartTime)
                .append(",get avg cost time:").append(getAvgTimes[getAvgCounter.get()] - getAvgTimes[0])
                .append(",get avg cost time1:").append(System.currentTimeMillis() - getAvgTimes[0]);

        sb.append(",getAvgCounter:").append(getAvgCounter.get()).append(",getMaxMsgNum:").append(getMaxMsgNum)
                .append(",getMsgCount:").append(getMsgCount).append("\n");
        sb.append("[gc]---------------------------------------------" + System.lineSeparator());
        sb.append(gcBuilder.toString() + System.lineSeparator());
        Utils.print(sb.toString());

        if (getAvgTimes[getAvgCounter.get()] - getAvgTimes[0] > Const.DEST) {
            System.err.println("func=shutdownHook stop");
        }
    }
}
