package io.openmessaging;

import io.netty.util.concurrent.FastThreadLocal;
import io.openmessaging.index.TAIndex;
import io.openmessaging.model.GetMsgItem;
import io.openmessaging.util.Utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static io.openmessaging.util.Utils.print;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageStoreImpl extends MessageStore {
    private static volatile boolean isFirstGet = true;

    static List<MessageFile> messageFiles = new ArrayList<>();
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

    private static FastThreadLocal<GetMsgItem> getMsgItemThreadLocal = new FastThreadLocal<GetMsgItem>() {
        @Override
        public GetMsgItem initialValue() {
            GetMsgItem item = new GetMsgItem();
            item.readBuf = ByteBuffer.allocate(Const.MAX_GET_AT_SIZE * Const.MSG_BYTES);
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
        iostat();
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
                    MessageFile.Iterator[] iterators = new MessageFile.Iterator[messageFiles.size()];
                    for (int i = messageFiles.size() - 1; i >= 0; i--) {
                        messageFiles.get(i).flush();
                        iterators[i] = messageFiles.get(i).iterator();
                    }
                    Monitor.getMsgStart();
                    Gather.init(iterators);
                    Gather.start();
                    Gather.join();
                    Utils.print("get msg start");
                    isFirstGet = false;
                }
            }
        }

        GetMsgItem getItem = getMsgItemThreadLocal.get();
        int messageFileSize = messageFiles.size();

        List<Message> messages = new ArrayList<>(Const.MAX_GET_MESSAGE_SIZE);
        getItem.messages = messages;
        for (int i = messageFileSize - 1; i >= 0; i--) {
            messageFiles.get(i).get(aMin, aMax, tMin, tMax, getItem);
        }

        messages.sort(messageComparator);

        Monitor.updateMaxMsgNum(messages.size());
        return messages;
    }


    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        Monitor.getAvgStat();
        return TAIndex.taIndex.getAvgValue(aMin, aMax, tMin, tMax);
    }

    private static void iostat() {
        try {

            Thread thread = new Thread(() -> {
                try {
                    String command = "iostat -xm 1";
                    Process p = Runtime.getRuntime().exec(command);

                    //p.waitFor();
                    InputStream is = p.getInputStream();
                    BufferedReader reader = new BufferedReader(new InputStreamReader(is));
                    String s;
                    while ((s = reader.readLine()) != null) {
                        System.out.println(s);
                    }

                } catch (Exception e) {
                    System.out.print(e.getMessage());
                }
            });
            thread.setDaemon(true);
            thread.start();

            //Process p=new ProcessBuilder(new String[]{"iostat","-xdm","1"}).start();
        } catch (Exception e) {
            System.out.print(e.getMessage());
        }
    }


    static {

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Monitor.log();
        }));
    }
}
