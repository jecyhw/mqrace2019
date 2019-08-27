package io.openmessaging;

/**
 * Created by yanghuiwei on 2019-07-26
 */
public interface Const {
    String STORE_PATH = "./data/";
    String MSG_FILE_SUFFIX = ".msg";
    String A_FILE_SUFFIX = ".a";
    int GET_AVG_COUNT = 310000;

    int MSG_BYTES = 34;

    int PUT_BUFFER_SIZE = 1024 * 1024; // 16k写入
    int GET_BUFFER_SIZE = 1024 * 1024 * 4; //一次最多读取
    int LONG_BYTES = 8;


    //固定成一块
    int MEMORY_BUFFER_SIZE = 1024 * 1024 * 42;

    int INDEX_INTERVAL = 64 * 64;
    int INDEX_ELE_LENGTH = 58000;//需要和INDEX_INTERVAL一起调
    //INDEX_INTERVAL 为48，INDEX_ELE_LENGTH=3700000
    //INDEX_INTERVAL 为64，INDEX_ELE_LENGTH=2800000
    //INDEX_INTERVAL 128，INDEX_ELE_LENGTH=1400000

    int T_DECREASE = 3; //取值范围 0-3

    int MAX_GET_AT_SIZE = 10 * 10000;
    int MAX_GET_MESSAGE_SIZE = 7 * 10000;

    int A_MEMORY_IN_HEAP_SIZE = 1024*1024 * 250;
    int A_MEMORY_IN_HEAP_NUM = 6;
    int A_MEMORY_OUT_HEAP_SIZE = 1024*1024*250;

    boolean PRINT_LOG = true;

    int DEST = 20000;
}
