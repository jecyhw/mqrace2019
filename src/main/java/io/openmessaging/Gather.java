package io.openmessaging;

import io.openmessaging.index.TAIndex;
import io.openmessaging.util.Utils;

import java.nio.ByteBuffer;

/**
 * Created by yanghuiwei on 2019-08-11
 */
public class Gather {

    private static GatherThread gatherThread;
    private static ByteBuffer aBuf = ByteBuffer.allocate(Const.INDEX_INTERVAL * Const.LONG_BYTES);

    public static void init(MessageFile.Iterator[] iterators) {
        Item[] items = new Item[iterators.length];
        for (int i = 0; i < items.length; i++) {
            items[i] = new Item();
            items[i].iterator = iterators[i];
        }

        gatherThread = new GatherThread(items);
    }

    public static void start() {
        gatherThread.start();;
    }

    public static void join() {
        try {
            gatherThread.join();
        } catch (InterruptedException e) {
            Utils.print("func=join " + e.getMessage());
            System.exit(-1);
        }
    }
    static class GatherThread extends Thread {
        Item[] items;
        public GatherThread(Item[] items) {
            this.items = items;
        }
        @Override
        public void run() {
            int mergeCount = 0;

            long startTime = System.currentTimeMillis();

            int size = items.length;
            //获取第一个元素进行初始化
            for (int i = 0; i < size; i++) {
                items[i].init();
            }

            long minT;
            int minIndex = 0;

            long[] ts = new long[Const.MERGE_T_INDEX_INTERVAL];
            long[] as = new long[Const.MERGE_T_INDEX_INTERVAL];
            int len = 0;

            while (size > 0) {
                //找最小元素以及对应的下标
                minT = Integer.MAX_VALUE;
                for (int i = 0; i < size; i++) {
                    long t = items[i].curT();
                    if (t < minT) {
                        minT = t;
                        minIndex = i;
                    }
                }

                Item item = items[minIndex];
                ts[len] = item.curT();
                as[len++] = item.nextA();

                if (len == Const.MERGE_T_INDEX_INTERVAL) {
                    TAIndex.flush(ts, as, len);
                    len = 0;

                    if (mergeCount++ % 1024 == 0) {
                        Utils.print("merging, cost time:" + (System.currentTimeMillis() - startTime) + " putCount:" + TAIndex.putCount + " merge count:" + mergeCount);
                    }
                }
                //获取最小元素的下一个值
                if (!item.hasNext()) {
                    //先减1，再删除
                    --size;
                    System.arraycopy(items, minIndex+1, items, minIndex, size - minIndex);
                }
            }

            TAIndex.flushEnd(ts, as, len);

            Utils.print("merge end, cost time:" + (System.currentTimeMillis() - startTime));
        }
    }

    static class Item {
        MessageFile.Iterator iterator;
        int index = 0;
        int len;
        long t[] = new long[Const.INDEX_INTERVAL];
        long a[] = new long[Const.INDEX_INTERVAL];

        void init() {
            len = iterator.nextTAndA(t, a, aBuf);
            index = 0;
        }

        long curT() {
            return t[index];
        }

        long nextA() {
            return a[index++];
        }

        boolean hasNext() {
            if (index == len) {
                boolean res = iterator.hasNext();
                if (res) {
                    len = iterator.nextTAndA(t, a, aBuf);
                    index = 0;
                }
                return res;
            }
            return true;
        }
    }
}
