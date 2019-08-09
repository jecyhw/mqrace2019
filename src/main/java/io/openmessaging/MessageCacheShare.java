package io.openmessaging;

import java.util.concurrent.atomic.AtomicInteger;

public class MessageCacheShare {
    private static int MAX_NUM = 1024*1024*8;
    private static Message[] messages;
    private static AtomicInteger idxCounter;


    public static void init() {
        idxCounter = new AtomicInteger(-1);
        messages = new Message[MAX_NUM];
        for (int i = 0; i < messages.length; i++){
            messages[i] = new Message(0,0,new byte[Const.MSG_BYTES]);
        }
    }

    public static Message get(){
        int idx = idxCounter.incrementAndGet() % MAX_NUM;
        return messages[idx];
    }
}