package io.openmessaging;

import io.netty.util.concurrent.FastThreadLocal;
import io.openmessaging.util.Utils;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.util.Utils.print;

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

    private static AtomicInteger getCounter = new AtomicInteger(0);
    private static GetItem[] items = new GetItem[12];

    private static FastThreadLocal<GetItem> getMsgItemThreadLocal = new FastThreadLocal<GetItem>() {
        @Override
        public GetItem initialValue() {
            GetItem item = new GetItem();
            int size = messageFiles.size();
            item.tBufs = new ByteBuffer[size];
            for (int i = 0; i < size; i++) {
                item.tBufs[i] = messageFiles.get(i).buf.duplicate();
            }

            items[getCounter.incrementAndGet() - 1] = item;
            return item;
        }
    };

    private static FastThreadLocal<GetItem> getAvgItemThreadLocal = new FastThreadLocal<GetItem>() {
        @Override
        public GetItem initialValue() {
            return items[getCounter.decrementAndGet()];
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
        Utils.print("getAvg aMin:" + Long.toBinaryString(aMin) + " aMax:" + Long.toBinaryString(aMax)
                + " tMin:" + Long.toBinaryString(tMin) + " tMax:" + Long.toBinaryString(tMax)
                + " diffA:" + (aMax - aMin) + " diffT:" + (tMax - tMin));
        if (isFirstGet) {
            synchronized (DefaultMessageStoreImpl.class) {
                if (isFirstGet) {
                    for (int i = messageFiles.size() - 1; i >= 0; i--) {
                        messageFiles.get(i).flush();
                    }
                    Monitor.getMsgStart();
                    isFirstGet = false;
                }
            }
        }
        GetItem getItem = getMsgItemThreadLocal.get();
        int messageFileSize = messageFiles.size();

        List<Message> messages = new ArrayList<>(Const.MAX_GET_MESSAGE_SIZE);
        getItem.messages = messages;
        for (int i = messageFileSize - 1; i >= 0; i--) {
            messageFiles.get(i).get(aMin, aMax, tMin, tMax, getItem, getItem.tBufs[i]);
        }

        messages.sort(messageComparator);

        Monitor.updateMaxMsgNum(messages.size());
        return messages;
    }

    private static volatile boolean isFirstGetAvg = true;

    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        Utils.print("getAvg aMin:" + Long.toBinaryString(aMin) + " aMax:" + Long.toBinaryString(aMax)
                + " tMin:" + Long.toBinaryString(tMin) + " tMax:" + Long.toBinaryString(tMax)
                + " diffA:" + (aMax - aMin) + " diffT:" + (tMax - tMin));

        if (isFirstGetAvg) {
            synchronized (DefaultMessageStoreImpl.class) {
                if (isFirstGetAvg) {
                    for (MessageFile messageFile : messageFiles) {
                        messageFile.readATime = 0;
                    }
                    Utils.print("getMsg readAFromFile:" + MessageFile.readAFromFile.get() + ",readAFromMemory:" + MessageFile.readAFromMemory.get());
                    MessageFile.readAFromFile.set(0);
                    MessageFile.readAFromMemory.set(0);
                    isFirstGetAvg = false;
                }
            }
        }

        GetItem getItem = getAvgItemThreadLocal.get();

        IntervalSum intervalSum = getItem.intervalSum;
        intervalSum.reset();
        for (int i = messageFiles.size() - 1; i >= 0; i--) {
            messageFiles.get(i).getAvgValue(aMin, aMax, tMin, tMax, intervalSum, getItem, getItem.tBufs[i]);
        }

        return intervalSum.avg();
    }

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            StringBuilder sb = new StringBuilder();
            for (MessageFile messageFile : messageFiles) {
                sb.append("[readATime:").append(messageFile.readATime).append("]");
            }
            sb.append("\n");
            Utils.print(sb.toString());
            Monitor.log();
        }));
    }
}
