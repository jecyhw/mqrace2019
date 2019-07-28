package io.openmessaging;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.Utils.dPrint;
import static io.openmessaging.Utils.print;


/**
 * Created by yanghuiwei on 2019-07-26
 */
public class FileMessageStore {
    private static volatile boolean isFirstGet = true;
    private static long getMaxT = Long.MIN_VALUE;
    private static long getMinT = Long.MAX_VALUE;
    private static long getDiffMaxT = Long.MIN_VALUE;
    private static long getDiffMinT = Long.MAX_VALUE;
    private static long getMaxA = Long.MIN_VALUE;
    private static long getMinA = Long.MAX_VALUE;
    private static long getDiffMaxA = Long.MIN_VALUE;
    private static long getDiffMinA = Long.MAX_VALUE;
    private static AtomicInteger getCount = new AtomicInteger(0);
    private static Set<Thread> getTheadSet = new HashSet<>();

    private static volatile boolean isFirstGetAvgValue = true;
    private static long getAvgMaxT = Long.MIN_VALUE;
    private static long getAvgMinT = Long.MAX_VALUE;
    private static long getAvgDiffMaxT = Long.MIN_VALUE;
    private static long getAvgDiffMinT = Long.MAX_VALUE;
    private static long getAvgMaxA = Long.MIN_VALUE;
    private static long getAvgMinA = Long.MAX_VALUE;
    private static long getAvgDiffMaxA = Long.MIN_VALUE;
    private static long getAvgDiffMinA = Long.MAX_VALUE;
    private static AtomicInteger getAvgCount = new AtomicInteger(0);
    private static Set<Thread> getAvgTheadSet = new HashSet<>();

    private static List<MessageFile> messageFiles = new ArrayList<>();
    private static ThreadLocal<MessageFile> messageFileThreadLocal = ThreadLocal.withInitial(() ->  {
        MessageFile messageFile = new MessageFile();
        messageFiles.add(messageFile);
        return messageFile;
    });

    public static void init() {
        //创建存储父目录
        File storeDir = new File(Const.STORE_PATH);
        if (!storeDir.exists()) {
            storeDir.mkdirs();
        }
    }

    public static void put(Message message) {
        messageFileThreadLocal.get().put(message);
    }

    public static List<Message> get(long aMin, long aMax, long tMin, long tMax) {
        firstGet(aMin, aMax, tMin, tMax);

        List<Message> messages = new ArrayList<>();
        for (MessageFile messageFile : messageFiles) {
            messages.addAll(messageFile.get(aMin, aMax, tMin, tMax));
        }
        return messages;
    }

    public static long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        firstGetAvgValue(aMin, aMax, tMin, tMax);
        long sum = 0;
        int count = 0;
        for (MessageFile messageFile : messageFiles) {
            IntervalSum intervalSum = messageFile.getAvgValue(aMin, aMax, tMin, tMax);
            sum += intervalSum.sum;
            count += intervalSum.count;
        }
        return sum / count;
    }

    private static void firstGet(long aMin, long aMax, long tMin, long tMax) {
        if (isFirstGet) {
            synchronized (FileMessageStore.class) {
                if (isFirstGet) {
                    isFirstGet = false;
                    for (MessageFile messageFile : messageFiles) {
                        print("func=firstGet---------------------------------------");
                        StringBuilder sb = new StringBuilder();
                        for (int i = 0; i < messageFile.chCount.length; i++) {
                            sb.append(i).append("=").append(messageFile.chCount[i]).append(" ");
                        }
                        dPrint(sb.toString());
                        dPrint("putMinT=" + messageFile.putMinT + " putMaxT=" + messageFile.putMaxT + " putMinA=" + messageFile.putMinA + " putMaxA=" + messageFile.putMaxA);
                        dPrint("messageCount=" + messageFile.messageCount + " idAllocator=" + MessageFile.idAllocator.get());
                        dPrint("isTSequence=" + messageFile.isTSequence + "isTEqual" + messageFile.isTEqual + " isByte=" + messageFile.isByte);
                        try {
                            dPrint("tFileSize=" + messageFile.tFc.size() + " aFileSize=" + messageFile.aFc.size() + " msgFileSize=" + messageFile.msgFc.size());
                        } catch (IOException e) {
                            e.printStackTrace();
                            print("func=firstGet error");
                        }
                    }
                }
            }
        }
        getStat(aMin, aMax, tMin, tMax);
    }

    private static void getStat(long aMin, long aMax, long tMin, long tMax) {
        long diffA = aMax - aMin, diffT = tMax - tMin;
        synchronized (FileMessageStore.class) {
            getTheadSet.add(Thread.currentThread());
            getMaxA = Math.max(getMaxA, aMax);
            getMinA = Math.min(getMinA, aMin);
            getMaxT = Math.max(getMaxT, tMax);
            getMinT = Math.min(getMinT, tMin);

            getDiffMaxA = Math.max(getDiffMaxA, diffA);
            getDiffMinA = Math.min(getDiffMinA, diffA);
            getDiffMaxT = Math.max(getDiffMaxT, diffT);
            getDiffMinT = Math.min(getDiffMinT, diffT);
        }

        if (getCount.getAndIncrement() % Const.PRINT_MSG_INTERVAL == 0) {
            print("func=getStat aMin=" + aMin + " aMax=" + aMax + " tMin=" + tMin + " tMax=" + tMax);
        }
    }

    private static void firstGetAvgValue(long aMin, long aMax, long tMin, long tMax) {
        if (isFirstGetAvgValue) {
            synchronized (FileMessageStore.class) {
                if (isFirstGetAvgValue) {
                    isFirstGetAvgValue = false;
                    print("func=firstGetAvgValue---------------------------------------");
                    dPrint("getMinT=" + getMinT + " getMaxT=" + getMaxT + " getMinA=" + getMinA + " getMaxA=" + getMaxA);
                    dPrint("getDiffMinT=" + getDiffMinT + " getDiffMaxT=" + getDiffMaxT + " getDiffMinA=" + getDiffMinA + " getDiffMaxA=" + getDiffMaxA);
                    dPrint("getThreadSize=" + getTheadSet.size() + " getCount=" + getCount.get());
                }
            }
        }
        getAvgValueStat(aMin, aMax, tMin, tMax);
    }

    private static void getAvgValueStat(long aMin, long aMax, long tMin, long tMax) {
        long diffA = aMax - aMin, diffT = tMax - tMin;
        synchronized (FileMessageStore.class) {
            getAvgTheadSet.add(Thread.currentThread());
            getAvgMaxA = Math.max(getAvgMaxA, aMax);
            getAvgMinA = Math.min(getAvgMinA, aMin);
            getAvgMaxT = Math.max(getAvgMaxT, tMax);
            getAvgMinT = Math.min(getAvgMinT, tMin);

            getAvgDiffMaxA = Math.max(getAvgDiffMaxA, diffA);
            getAvgDiffMinA = Math.min(getAvgDiffMinA, diffA);
            getAvgDiffMaxT = Math.max(getAvgDiffMaxT, diffT);
            getAvgDiffMinT = Math.min(getAvgDiffMinT, diffT);
        }

        if (getCount.getAndIncrement() % Const.PRINT_MSG_INTERVAL == 0) {
            print("func=getStat aMin=" + aMin + " aMax=" + aMax + " tMin=" + tMin + " tMax=" + tMax);
        }
    }

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run()
            {
                print("func=shutdownHook---------------------------------------");
                dPrint("getAvgMinT=" + getAvgMinT + " getAvgMaxT=" + getAvgMaxT + " getAvgMinA=" + getAvgMinA + " getAvgMaxA=" + getAvgMaxA);
                dPrint("getAvgDiffMinT=" + getAvgDiffMinT + " getAvgDiffMaxT=" + getAvgDiffMaxT + " getAvgDiffMinA=" + getAvgDiffMinA + " getAvgDiffMaxA=" + getAvgDiffMaxA);
                dPrint("getAvgThreadSize=" + getAvgTheadSet.size() + " getAvgCount=" + getAvgCount.get());
            }
        }));
    }
}
