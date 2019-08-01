package io.openmessaging;

import java.util.List;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageStoreImpl extends MessageStore {

    public DefaultMessageStoreImpl() {
        Monitor.init();
        FileMessageStore.init();
    }

    @Override
    public void put(Message message) {
        FileMessageStore.put(message);
    }


    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        return FileMessageStore.get(aMin, aMax, tMin, tMax);
    }


    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        return FileMessageStore.getAvgValue(aMin, aMax, tMin, tMax);
    }

}
