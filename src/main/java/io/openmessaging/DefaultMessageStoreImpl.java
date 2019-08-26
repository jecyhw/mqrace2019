package io.openmessaging;

import io.netty.util.concurrent.FastThreadLocal;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

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

    private static FastThreadLocal<GetItem> getBufThreadLocal = new FastThreadLocal<GetItem>() {
        @Override
        public GetItem initialValue() {
            GetItem item = new GetItem();
            int size = messageFiles.size();
            item.tBufs = new ByteBuffer[size];
            for (int i = 0; i < size; i++) {
                item.tBufs[i] = messageFiles.get(i).buf.duplicate();
            }
            return item;
        }
    };

    private static Comparator<Message> messageComparator = new Comparator<Message>() {
        @Override
        public int compare(Message o1, Message o2) {
            return Long.compare(o1.getT(), o2.getT());
        }
    };

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
        if (isFirstGet) {
            synchronized (DefaultMessageStoreImpl.class) {
                if (isFirstGet) {
                    for (int i = messageFiles.size() - 1; i >= 0; i--) {
                        messageFiles.get(i).flush();
                    }

                    long[] a = new long[256];
                    long byteLen = 0, shortLen = 0, intLen = 0, longLen = 0;

                    for (int i = messageFiles.size() - 1; i >= 0; i--) {
                        MessageFile messageFile = messageFiles.get(i);
                        long[] stat = messageFile.aEncoder.stat;
                        for (int j = 0; j < stat.length; j++) {
                            a[j] += stat[j];
                        }
                        BodyEncoder bodyEncoder = messageFile.bodyEncoder;
                        byteLen += bodyEncoder.byteLen;
                        shortLen += bodyEncoder.shortLen;
                        intLen += bodyEncoder.intLen;
                        longLen += bodyEncoder.longLen;
                    }
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < a.length; i++) {
                        if (a[i] == 0) {
                            continue;
                        }
                        sb.append("[").append(i).append(",").append(a[i]).append("]");
                    }
                    sb.append("\n");
                    sb.append("byteLen").append(byteLen).append("shortLen").append(shortLen)
                            .append("intLen").append(intLen).append("longLen").append(longLen).append("\n");


                    Utils.print(sb.toString());

                    Monitor.getMsgStart();
                    isFirstGet = false;
                }
//                List<Message> messages = new ArrayList<>();
//                for (int i = messageFiles.size() - 1; i >= 0; i--) {
//                    messages.addAll(messageFiles.get(i).get(0, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, getItem));
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
//                System.out.println();
//                getAvgValue(6740781, 6840781, 6763632, 6778579);
            }
        }
        GetItem getItem = getBufThreadLocal.get();
        int messageFileSize = messageFiles.size();

        List<Message> messages = new ArrayList<>(Const.MAX_GET_MESSAGE_SIZE);
        getItem.messages = messages;
        for (int i = messageFileSize - 1; i >= 0; i--) {
            messageFiles.get(i).get(aMin, aMax, tMin, tMax, getItem, getItem.tBufs[i]);
        }


        messages.sort(messageComparator);
//        long min = Math.max(aMin, tMin), max= Math.min(aMax, tMax);
//        int count = (int) (max - min + 1);
//        while (min <= max) {
//            if ((min & 1) == 0) {
//                count++;
//            }
//            min++;
//        }
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


        GetItem getItem = getBufThreadLocal.get();

        IntervalSum intervalSum = getItem.intervalSum;
        intervalSum.reset();
        for (int i = messageFiles.size() - 1; i >= 0; i--) {
            messageFiles.get(i).getAvgValue(aMin, aMax, tMin, tMax, intervalSum, getItem, getItem.tBufs[i]);
        }

//        long max = Math.min(tMax, aMax);
//        long min = Math.max(tMin, aMin);
//        long count = 0;
//        long sum = 0;
//
//        if (min <= max) {
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
//            System.err.println(Thread.currentThread().getName() + " value check count:" + count + " sum:" + sum + " c:" + intervalSum.count + " s:" + intervalSum.sum + " aMin:" + aMin
//            + " aMax:" + aMax + " tMin:" + tMin + " tMax:" + tMax);
//        }



        return intervalSum.avg();
    }

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Monitor.log();
        }));
    }
}
