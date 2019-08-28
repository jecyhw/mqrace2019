package io.openmessaging;

import io.netty.util.concurrent.FastThreadLocal;

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
    private static GetItem[] items;

    private static FastThreadLocal<GetItem> getMsgItemThreadLocal = new FastThreadLocal<GetItem>() {
        @Override
        public GetItem initialValue() {
            GetItem item = new GetItem();
            int size = messageFiles.size();
            item.tBufs = new ByteBuffer[size];
            for (int i = 0; i < size; i++) {
                item.tBufs[i] = messageFiles.get(i).tBuf.duplicate();
            }
            int index = getCounter.incrementAndGet() - 1;
            item.buf = messageFiles.get(index).buf;
            items[index] = item;
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
        if (isFirstGet) {
            synchronized (DefaultMessageStoreImpl.class) {
                if (isFirstGet) {
                    for (int i = messageFiles.size() - 1; i >= 0; i--) {
                        messageFiles.get(i).flush();
                    }
                    Monitor.getMsgStart();
                    items = new GetItem[messageFiles.size()];

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


    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        Monitor.getAvgStat();
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
            Monitor.log();
        }));
    }
}
