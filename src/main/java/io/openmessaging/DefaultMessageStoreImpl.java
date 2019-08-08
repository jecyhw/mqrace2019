package io.openmessaging;

import io.netty.util.concurrent.FastThreadLocal;

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
    private static FastThreadLocal<MessageFile> messageFileThreadLocal = new FastThreadLocal<MessageFile>() {
        @Override
        public MessageFile initialValue()
        {
            MessageFile messageFile = new MessageFile();
            synchronized (DefaultMessageStoreImpl.class) {
                messageFiles.add(messageFile);
            }
            return messageFile;
        }
    };

    private static AtomicInteger getThreadCounter = new AtomicInteger(0);
    private static List<GetItem> getItems = new ArrayList<>();
    private static FastThreadLocal<GetItem> getBufThreadLocal = new FastThreadLocal<GetItem>() {
        @Override
        public GetItem initialValue() {
            GetItem item = new GetItem();
            getThreadCounter.incrementAndGet();
            synchronized (DefaultMessageStoreImpl.class) {
                getItems.add(item);
            }
            return item;
        }
    };

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

        Monitor.putStart();
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
        GetItem getItem = getBufThreadLocal.get();

        if (isFirstGet) {
            synchronized (DefaultMessageStoreImpl.class) {
                for (MessageFile messageFile : messageFiles) {
                    messageFile.flush();
                }
                Monitor.getMsgStart();

                isFirstGet = false;
//                List<Message> messages = new ArrayList<>();
//                for (int i = messageFiles.size() - 1; i >= 0; i--) {
//                    messages.addAll(messageFiles.get(i).get(1032358, 1132358, 1082705, 1139032, getItem));
//                }
//                messages.sort(messageComparator);
//                for (int i = 0; i < messages.size(); i++) {
//                    Message message = messages.get(i);
//                    if (message.getT() != message.getA() || message.getT() != ByteBuffer.wrap(message.getBody()).getLong()) {
//                        System.err.println("1.error");
//                    }
//                    if ((message.getT() & 1) == 0) {
//                        if (message.getT() != messages.get(i + 1).getT()) {
//                            System.err.println("2.error");
//                        }
//                        if (i > 0) {
//                            if (message.getT() != messages.get(i - 1).getT() + 1) {
//                                System.err.println("3.error");
//                            }
//                        }
//                        i++;
//                    } else {
//                        if ( i > 0 && message.getT() != messages.get(i - 1).getT() + 1) {
//                            System.err.println("4.error");
//                        }
//                    }
//                }
//                getAvgValue(6740781, 6840781, 6763632, 6778579);
            }
        }

        List<Message> messages = new ArrayList<>();
        for (int i = messageFiles.size() - 1; i >= 0; i--) {
            messages.addAll(messageFiles.get(i).get(aMinInt, aMaxInt, tMinInt, tMaxInt, getItem));
        }
        messages.sort(messageComparator);
//        int min = Math.max(aMinInt, tMinInt), max= Math.min(aMaxInt, tMaxInt);
//        int count = max - min + 1;
//        while (min <= max) {
//            if ((min & 1) == 0) {
//                count++;
//            }
//            min++;
//        }
//
//        if (messages.size() != count) {
//            System.err.println("6.error");
//        }

        Monitor.updateMaxMsgNum(messages.size());
        return messages;
    }


    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        Monitor.getAvgStat();

        int aMinInt = (int)aMin, aMaxInt = (int)aMax, tMinInt = (int)tMin, tMaxInt = (int)tMax;

        GetItem getItem = getBufThreadLocal.get();

        IntervalSum intervalSum = getItem.intervalSum;
        intervalSum.count = 0;
        intervalSum.sum = 0;
        for (int i = messageFiles.size() - 1; i >= 0; i--) {
            messageFiles.get(i).getAvgValue(aMinInt, aMaxInt, tMinInt, tMaxInt, intervalSum);
        }

//        long max = Math.min(tMax, aMax);
//        long min = Math.max(tMin, aMin);
//        long count = 0, c1 = 0;
//        long sum = 0;
//        for (int i = 0; i < min; i++) {
//            if ((i & 1) == 0) {
//                c1++;
//            }
//            c1++;
//        }
//
//        if (min < max) {
//            count = max - min + 1;
//            while (min <= max) {
//                if ((min & 1) == 0) {
//                    count++;
//                    sum += min;
//                }
//                sum += min;
//                min++;
//            }
//        }
//
//        if (count != intervalSum.count || sum != intervalSum.sum) {
//            System.err.println("value check count:" + count + " sum:" + sum + " c:" + intervalSum.count + " s:" + intervalSum.sum + " aMin:" + aMin
//            + " aMax:" + aMax + " tMin:" + tMin + " tMax:" + tMax);
//        }

        if (intervalSum.count == 0) {
            return 0;
        }

        return intervalSum.sum / intervalSum.count;
    }

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Monitor.log();
            System.err.println("func=shutdownHook stop");
        }));
    }
}
