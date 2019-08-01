package io.openmessaging;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.Utils.dPrint;
import static io.openmessaging.Utils.print;


/**
 * Created by yanghuiwei on 2019-07-26
 */
public class FileMessageStore {

    private static volatile boolean isFirstGet = true;
    private static GetStat getStat = new GetStat();

    private static volatile boolean isFirstGetAvgValue = true;
    private static GetStat getAvgStat = new GetStat();
    private static AtomicInteger robin = new AtomicInteger(0);


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
        messageFileThreadLocal.get().put(message);
    }

    public static List<Message> get(long aMin, long aMax, long tMin, long tMax) {
        firstGet(aMin, aMax, tMin, tMax);

        List<List<Message>> messagesList = new ArrayList<>(messageFiles.size());
        GetItem getItem = getBufThreadLocal.get();

        int messageSize = 0;

        for (int i = 0, pos = robin.getAndIncrement(); i < messageFiles.size(); i++) {
            List<Message> messages = messageFiles.get(pos % messageFiles.size()).get(aMin, aMax, tMin, tMax, getItem);
            messagesList.add(messages);
            messageSize += messages.size();
            pos++;
        }

        return sort(messagesList, messageSize);
    }

    private static List<Message> sort(List<List<Message>> messagesList, int size) {
        if (messagesList.isEmpty()) {
            return Collections.emptyList();
        }
        if (messagesList.size() == 1) {
            return messagesList.get(0);
        }
        List<Message> messages = messagesList.get(0);
        for (int i = 1; i < messagesList.size(); i++) {
            messages = merge(messages, messagesList.get(i));
        }
        return messages;
    }

    private static List<Message> merge(List<Message> messages1, List<Message> messages2) {
        List<Message> messages = new ArrayList<>(messages1.size() + messages2.size());
        int i1 = 0, i2 = 0;
        while (i1 < messages1.size() && i2 < messages2.size()) {
            Message message1 = messages1.get(i1);
            Message message2 = messages2.get(i2);
            if (message1.getT() < message2.getT()) {
                messages.add(message1);
                i1++;
            } else {
                messages.add(message2);
                i2++;
            }
        }

        if (i1 < messages1.size()) {
            messages.addAll(messages1.subList(i1, messages1.size()));
        } else {
            messages.addAll(messages2.subList(i2, messages2.size()));
        }
        return messages;
    }

    public static long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        firstGetAvgValue(aMin, aMax, tMin, tMax);
        long sum = 0;
        int count = 0;
        GetItem getItem = getBufThreadLocal.get();
        for (MessageFile messageFile : messageFiles) {
            IntervalSum intervalSum = messageFile.getAvgValue(aMin, aMax, tMin, tMax, getItem);
            sum += intervalSum.sum;
            count += intervalSum.count;
        }
        return sum / count;
    }

    private static void firstGet(long aMin, long aMax, long tMin, long tMax) {
        if (isFirstGet) {
            synchronized (FileMessageStore.class) {
                if (isFirstGet) {
                    for (MessageFile messageFile : messageFiles) {
                        messageFile.flush();
                        printPutStat(messageFile);
                    }
                    isFirstGet = false;
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
                print("func=shutdownHook---------------------------------------");

                for (MessageFile messageFile : messageFiles) {
                    printPutStat(messageFile);
                }
                printStat(getStat);
                printStat(getAvgStat);

                System.err.println("func=shutdownHook stop");
            }
        }));
    }

    private static void printPutStat(MessageFile messageFile) {
        print("func=printPutStat---------------------------------------");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < messageFile.chCount.length; i++) {
            sb.append(i).append("=").append(messageFile.chCount[i]).append(" ");

        }
        dPrint(sb.toString());
        dPrint("putMinT=" + messageFile.putMinT + " putMaxT=" + messageFile.putMaxT + " putMinA=" + messageFile.putMinA + " putMaxA=" + messageFile.putMaxA);
        dPrint("messageCount=" + messageFile.messageCount + " idAllocator=" + MessageFile.idAllocator.get());
        dPrint("isTSequence=" + messageFile.isTSequence + " isTEqual" + messageFile.isTEqual + " isByte=" + messageFile.isByte + " maxTInterval=" + messageFile.maxTInterval);
        try {
            dPrint("tFileSize=" + messageFile.tFc.size() + " aFileSize=" + messageFile.aFc.size() + " msgFileSize=" + messageFile.msgFc.size());
        } catch (IOException e) {
            print("func=firstGet error " + e.getMessage());
        }
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
