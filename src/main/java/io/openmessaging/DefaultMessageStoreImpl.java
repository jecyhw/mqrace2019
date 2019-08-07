package io.openmessaging;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.Utils.print;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageStoreImpl extends MessageStore {
    private static volatile boolean isFirstGet = true;

    private static List<MessageFile> messageFiles = new ArrayList<>();
    private static ThreadLocal<MessageFile> messageFileThreadLocal = ThreadLocal.withInitial(() ->  {
        MessageFile messageFile = new MessageFile();
        synchronized (DefaultMessageStoreImpl.class) {
            messageFiles.add(messageFile);
        }
        return messageFile;
    });

    private static AtomicInteger getThreadCounter = new AtomicInteger(0);
    private static List<GetItem> getItems = new ArrayList<>();
    private static ThreadLocal<GetItem> getBufThreadLocal = ThreadLocal.withInitial(() -> {
        GetItem item = new GetItem();
        getThreadCounter.incrementAndGet();
        synchronized (DefaultMessageStoreImpl.class) {
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
        MessageCacheShare.init();
        print("func=init success");
    }

    public DefaultMessageStoreImpl() {
        init();
    }

    @Override
    public void put(Message message) {
        messageFileThreadLocal.get().put(message);
    }


    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        int aMinInt = (int)aMin, aMaxInt = (int)aMax, tMinInt = (int)tMin, tMaxInt = (int)tMax;

        if (isFirstGet) {
            synchronized (DefaultMessageStoreImpl.class) {
                for (MessageFile messageFile : messageFiles) {
                    messageFile.flush();
                }
                isFirstGet = false;
            }
        }

        List<Message> messages = new ArrayList<>();
        GetItem getItem = getBufThreadLocal.get();


        for (int i = messageFiles.size() - 1; i >= 0; i--) {
            messages.addAll(messageFiles.get(i).get(aMinInt, aMaxInt, tMinInt, tMaxInt, getItem));
        }

        messages.sort(messageComparator);
        return messages;
    }


    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        long sum = 0;
        int count = 0;
        GetItem getItem = getBufThreadLocal.get();

        for (int i = messageFiles.size() - 1; i >= 0; i--) {
            IntervalSum intervalSum = messageFiles.get(i).getAvgValue(aMin, aMax, tMin, tMax, getItem);
            sum += intervalSum.sum;
            count += intervalSum.count;
        }

        if (count == 0) {
            return 0;
        }
        return sum / count;
    }

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            MemoryIndex.log();
                System.err.println("func=shutdownHook stop");
        }));
    }
}
