package io.openmessaging;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.Utils.dPrint;
import static io.openmessaging.Utils.print;


/**
 * Created by yanghuiwei on 2019-07-26
 */
public class FileMessageStore {
    private static volatile boolean isFirstWrite = true;

    private static volatile boolean isFirstGet = true;
    private static GetStat getStat = new GetStat();

    private static volatile boolean isFirstGetAvgValue = true;
    private static GetStat getAvgStat = new GetStat();

    public static List<Info> infoList = new ArrayList<>();
    private static AtomicInteger tidCounter = new AtomicInteger(0);
    private static ThreadLocal<Info> infoThreadLocal = ThreadLocal.withInitial(() ->  {
        Info info = new Info(tidCounter.getAndIncrement());
        infoList.add(info);
        return info;
    });

    private static List<MessageFile> messageFiles = new ArrayList<>();
    private static ThreadLocal<MessageFile> messageFileThreadLocal = ThreadLocal.withInitial(() ->  {
        MessageFile messageFile = new MessageFile();
        synchronized (FileMessageStore.class) {
            messageFiles.add(messageFile);
        }
        return messageFile;
    });

    private static AtomicInteger getThreadCounter = new AtomicInteger(0);
    private static List<GetItem> getItems = new ArrayList<>();
    private static ThreadLocal<GetItem> getBufThreadLocal = ThreadLocal.withInitial(() -> {
        GetItem item = new GetItem();
        getThreadCounter.incrementAndGet();
        synchronized (FileMessageStore.class) {
            getItems.add(item);
        }
        return item;
    });

    private static Comparator<Message> messageComparator = Comparator.comparingLong(Message::getT);

    public static void init() {
        //创建存储父目录
        File storeDir = new File(Const.STORE_PATH);
        if (!storeDir.exists()) {
            storeDir.mkdirs();
        } else if (storeDir.isFile()) {
            storeDir.delete();
            storeDir.mkdirs();
        } else {
            for (File file : storeDir.listFiles()) {
                file.delete();
            }
        }
        print("func=init success");
    }

    public static void put(Message message) {
        firstWrite(message);
        Monitor.writeStage(message);
        infoThreadLocal.get().cal(message);
        messageFileThreadLocal.get().put(message);
    }

    public static List<Message> get(long aMin, long aMax, long tMin, long tMax) {
        firstGet(aMin, aMax, tMin, tMax);

        List<Message> messages = new ArrayList<>();
        GetItem getItem = getBufThreadLocal.get();

        for (int i = messageFiles.size() - 1; i >= 0; i--) {
            messages.addAll(messageFiles.get(i).get(aMin, aMax, tMin, tMax, getItem));
        }

        Monitor.getMessageStage( aMin,  aMax, tMin,  tMax, messages.size());
        messages.sort(messageComparator);
        return messages;
    }


    public static long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        firstGetAvgValue(aMin, aMax, tMin, tMax);

        long sum = 0;
        int count = 0;
        GetItem getItem = getBufThreadLocal.get();
        for (int i = messageFiles.size() - 1; i >= 0; i--) {
            IntervalSum intervalSum = messageFiles.get(i).getAvgValue(aMin, aMax, tMin, tMax, getItem);
            sum += intervalSum.sum;
            count += intervalSum.count;
        }
        Monitor.getAvgStage( aMin, aMax, tMin, tMax, count);
        return sum / count;
    }

    private static void firstWrite(Message message) {

        if (isFirstWrite) {
            synchronized (FileMessageStore.class) {
                if (isFirstWrite) {
                    Monitor.firstPid = Thread.currentThread().getId();
                    Monitor.mark(0);
                    isFirstWrite = false;
                }
            }
        }
    }

    private static void firstGet(long aMin, long aMax, long tMin, long tMax) {
        if (isFirstGet) {
            synchronized (FileMessageStore.class) {
                if (isFirstGet) {
                    Monitor.firstPid = Thread.currentThread().getId();
                    Monitor.mark(1);

                    for (MessageFile messageFile : messageFiles) {
                        messageFile.flush();
                        printPutStat(messageFile);
                    }
                    isFirstGet = false;

//                    int size = 0;
//                    int[][] data = new int[messageFiles.size()][];
//                    int len = 0;
//                    for (MessageFile messageFile : messageFiles) {
//                        MemoryGetItem sItem = new MemoryGetItem();
//                        MemoryGetItem eItem = new MemoryGetItem();
//                        eItem.nextMemIndex = messageFile.memoryIndex.memoryPos + 1;
//                        eItem.pos = messageFile.memoryIndex.putCount;
//                        int[] t = messageFile.memoryIndex.range(sItem, eItem, new MemoryRead());
//                        size += t.length;
//                        for (int i = 0; i < t.length; i++) {
//                            if (t[i] % 2 == 0) {
//                                if (t[i + 1] != t[i]) {
//                                    System.err.println("flush error");
//                                }
//                                i++;
//                            }
//                        }
//                        data[len++] = t;
//                    }
//
//                    int[] all = new int[size];
//                    len = 0;
//                    for (int[] t : data) {
//                        for (int i : t) {
//                            all[len++] = i;
//                        }
//                    }
//
//                    Arrays.sort(all);
//
//                    for (int i = 0; i < all.length; i++) {
//                        if (all[i] % 2 == 0) {
//                            if (all[i + 1] != all[i]) {
//                                System.err.println("flush error");
//                            }
//                            i++;
//                        }
//                    }
//
//                    System.out.println();
                }
            }
        }
        getStat(aMin, aMax, tMin, tMax);
    }

    private static void getStat(long aMin, long aMax, long tMin, long tMax) {
        stat(getStat, aMin, aMax, tMin, tMax);
    }

    private static void firstGetAvgValue(long aMin, long aMax, long tMin, long tMax) {
        if (isFirstGetAvgValue) {
            synchronized (FileMessageStore.class) {
                if (isFirstGetAvgValue) {
                    isFirstGetAvgValue = false;
                    Monitor.firstPid = Thread.currentThread().getId();
                    Monitor.mark(2);

                    printStat(getStat);
                }
            }
        }
        getAvgValueStat(aMin, aMax, tMin, tMax);
    }

    private static void getAvgValueStat(long aMin, long aMax, long tMin, long tMax) {
        stat(getAvgStat, aMin, aMax, tMin, tMax);
    }

    private static void stat(GetStat stat, long aMin, long aMax, long tMin, long tMax) {
        long diffA = aMax - aMin, diffT = tMax - tMin;
        synchronized (FileMessageStore.class) {
            stat.maxA = Math.max(stat.maxA, aMax);
            stat.minA = Math.min(stat.minA, aMin);
            stat.maxT = Math.max(stat.maxT, tMax);
            stat.minT = Math.min(stat.minT, tMin);

            stat.diffMaxA = Math.max(stat.diffMaxA, diffA);
            stat.diffMinA = Math.min(stat.diffMinA, diffA);
            stat.diffMaxT = Math.max(stat.diffMaxT, diffT);
            stat.diffMinT = Math.min(stat.diffMinT, diffT);
        }

        if (stat.count.getAndIncrement() % Const.PRINT_MSG_INTERVAL == 0) {
            print("func=stat aMin=" + aMin + " aMax=" + aMax + " tMin=" + tMin + " tMax=" + tMax);
        }
    }

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run()
            {
                Monitor.mark(3);
//                Monitor.print();

//                print("func=shutdownHook---------------------------------------");
//
//                for (MessageFile messageFile : messageFiles) {
//                    printPutStat(messageFile);
//                }
//                printStat(getStat);
//                printStat(getAvgStat);

                System.err.println("func=shutdownHook stop");
            }
        }));
    }

    private static void printPutStat(MessageFile messageFile) {
        print("func=printPutStat---------------------------------------");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < messageFile.chCount.length; i++) {
            sb.append(i).append("=").append(messageFile.chCount[i]).append(",");

        }
        dPrint(sb.toString());
        dPrint("putMinT=" + messageFile.putMinT + " putMaxT=" + messageFile.putMaxT + " putMinA=" + messageFile.putMinA + " putMaxA=" + messageFile.putMaxA);
        dPrint("messageCount=" + messageFile.messageCount + " idAllocator=" + MessageFile.idAllocator.get());
        dPrint("isTSequence=" + messageFile.isTSequence + " isTEqual" + messageFile.isTEqual + " isByte=" + messageFile.isByte + " maxTInterval=" + messageFile.maxTInterval);
        try {
            dPrint(" aFileSize=" + messageFile.aFc.size() + " msgFileSize=" + messageFile.msgFc.size());
        } catch (IOException e) {
            print("func=firstGet error " + e.getMessage());
        }
        sb = new StringBuilder();
        for (int i = 0; i < messageFile.aIntervals.length; i++) {
            if (messageFile.aIntervals[i] > 0) {
                sb.append(i - 35000).append("=").append(messageFile.aIntervals[i]).append(",");
            }
        }
        dPrint(sb.toString());
    }


    private static void printStat(GetStat stat) {
        print("func=printStat---------------------------------------");
        dPrint("minT=" + stat.minT + " maxT=" + stat.maxT + " minA=" + stat.minA + " maxA=" + stat.maxA);
        dPrint("diffMinT=" + stat.diffMinT + " diffMaxT=" + stat.diffMaxT + " diffMinA=" + stat.diffMinA + " diffMaxA=" + stat.diffMaxA);
        dPrint("theadSetSize=" + getThreadCounter.get() + " getCount=" + stat.count.get());

        StringBuilder sb = new StringBuilder();
        for (GetItem item : getItems) {
            sb.append("maxC=")
                    .append(item.maxCount)
                    .append(" maxAC=")
                    .append(item.maxActualCount)
                    .append(" ");
        }
        dPrint(sb.toString());
    }
}
