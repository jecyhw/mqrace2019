package io.openmessaging;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *  @Author: zhbitcxy@gmail.com
 *  @Date: 2019/7/31
 *  @Description:
 */
public class Monitor {
    public static long STAGE_TIME = System.currentTimeMillis();
    private static int TIMEOUT = 1000;
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
                        for (int i = 0; i < FileMessageStore.infoList.size(); i++){
                            FileMessageStore.infoList.get(i).print();
                        }
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

    private static AtomicInteger writeTotal = new AtomicInteger(0);
    public static void writeStage(Message message){
        countTInfoForOneThread(message);
        writeTotal.incrementAndGet();
    }

    private static AtomicInteger getMsgCnt = new AtomicInteger(0);
    private static AtomicInteger getMsgTotal = new AtomicInteger(0);
    public static void getMessageStage(long aMin, long aMax, long tMin, long tMax, int messageSize){
        getMsgCnt.incrementAndGet();
        getMsgTotal.addAndGet(messageSize);
    }

    private static AtomicInteger getAvgCnt = new AtomicInteger(0);
    private static AtomicInteger getAvgTotal = new AtomicInteger(0);
    public static void getAvgStage(long aMin, long aMax, long tMin, long tMax, int count){
        getAvgCnt.incrementAndGet();
        getAvgTotal.addAndGet(count);
    }

    public static void countTInfoForOneThread(Message message){
        if(firstPid == Thread.currentThread().getId()){

            if(lastT != -1){
                int interval = (int)(message.getT() - lastT);
                if (interval > tMaxInterval){
                    tMaxInterval = interval;
                }
            }

            if (message.getT() != lastT){
                uniqueTNum++;
                lastT = message.getT();
            }
        }
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

    private static volatile boolean monitorIoFirst = true;
    public static void iostatForDisk() throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(Runtime.getRuntime().exec("iostat -d -x -m /dev/nvme0n1 /dev/sda").getInputStream()));
        bufferedReader.readLine();
        bufferedReader.readLine();
        if(monitorIoFirst){
            iostatForDiskBuilder.append(bufferedReader.readLine() + "   timestamp");
            iostatForDiskBuilder.append(System.lineSeparator());
            monitorIoFirst = false;
        }else{
            bufferedReader.readLine();
        }
        String line = bufferedReader.readLine();
        int cnt = 0;
        while(line != null && !line.isEmpty() && cnt++ < 10){
            iostatForDiskBuilder.append(line  + "    " + System.currentTimeMillis());
            iostatForDiskBuilder.append(System.lineSeparator());
            line = bufferedReader.readLine();
        }
    }

    private static volatile boolean monitorCpuFirst = true;
    public static void iostatForCpu() throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(Runtime.getRuntime().exec("iostat -c").getInputStream()));
        bufferedReader.readLine();
        bufferedReader.readLine();
        if(monitorCpuFirst){
            iostatForCpuBuilder.append(bufferedReader.readLine()  + "   timestamp");
            iostatForCpuBuilder.append(System.lineSeparator());
            monitorCpuFirst = false;
        }else{
            bufferedReader.readLine();
        }
        iostatForCpuBuilder.append(bufferedReader.readLine()  + "    " + System.currentTimeMillis());
        iostatForCpuBuilder.append(System.lineSeparator());
    }

    private static volatile boolean monitorFreeFirst = true;
    public static void free() throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(Runtime.getRuntime().exec("free -m").getInputStream()));

        if(monitorFreeFirst){
            freeBuilder.append(bufferedReader.readLine() + "   timestamp");
            freeBuilder.append(System.lineSeparator());
            monitorFreeFirst = false;
        }else{
            bufferedReader.readLine();
        }
        freeBuilder.append(bufferedReader.readLine() + "    " + System.currentTimeMillis());
        freeBuilder.append(System.lineSeparator());
        freeBuilder.append(bufferedReader.readLine() + "    " + System.currentTimeMillis());
        freeBuilder.append(System.lineSeparator());
    }

    public static void print(){
//        System.out.println("[monitor]---------------------------------------------");
//        System.out.println(stageBuilder.toString());
//        System.out.println("[info on write stage] uniqueTNum:" + uniqueTNum + ", tMaxInterval:" + tMaxInterval + ", writeTotal:" + writeTotal.get());
//        System.out.println("[info on get stage]" +  " getMsgMethodCnt:" + getMsgCnt.get() + ", getAvgMethodCnt:" + getAvgCnt.get()
//        + ", getMsgTotal:" + getMsgTotal.get() + ", getAvgTotal:" + getAvgTotal.get()
//        );
//        System.out.println("==================================score======================================");
        long score1 = (scores[0] > 0 ? writeTotal.get()/scores[0]:0);
        long score2 = (scores[1] > 0 ? getMsgTotal.get()/scores[1]:0);
        long score3 = (scores[2] > 0 ? getAvgTotal.get()/scores[2]:0);
        System.out.println("发送阶段:" +  score1 + " (" + scores[0] + "ms)" );
        System.out.println("查询消息:" + score2 + " (" + scores[1] + "ms)");
        System.out.println("查询平均值:" + score3 + " (" + scores[2] + "ms)");
        System.out.println("score:" + (score1+score2+score3) + System.lineSeparator());
//        System.out.println("==================================score======================================");
//        System.out.println("[gc]---------------------------------------------");
//        System.out.println(gcBuilder.toString());
//        System.out.println("[iostat disk]-------------------------------------------");
//        System.out.println(iostatForDiskBuilder.toString());
//        System.out.println("[iostat cpu]-------------------------------------------");
//        System.out.println(iostatForCpuBuilder.toString());
//        System.out.println("[free]-------------------------------------------");
//        System.out.println(freeBuilder.toString());
    }
}

