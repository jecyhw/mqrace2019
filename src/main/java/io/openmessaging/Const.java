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

    int PUT_BUFFER_SIZE = 1024 * 1024 * 2; // 16k写入
    int LONG_BYTES = 8;


    //固定成一块
    int MEMORY_BUFFER_SIZE = 1024 * 1024 * 42;


    int INDEX_INTERVAL = 64;
    int INDEX_ELE_LENGTH = 3000000;//需要和INDEX_INTERVAL一起调
    //INDEX_INTERVAL 为48，INDEX_ELE_LENGTH=3700000
    //INDEX_INTERVAL 为64，INDEX_ELE_LENGTH=2800000
    //INDEX_INTERVAL 128，INDEX_ELE_LENGTH=1400000

    int COMPRESS_MSG_SIZE = INDEX_INTERVAL * Const.MSG_BYTES;

    int T_DECREASE = 3; //取值范围 0-3

    int MAX_GET_AT_SIZE = 8 * 10000;
    int MAX_GET_MESSAGE_SIZE = 7 * 10000;

    int A_MEMORY_IN_HEAP_SIZE = 1024 * 1024 * 400;
    int A_MEMORY_IN_HEAP_NUM = 7;
    int A_MEMORY_OUT_HEAP_SIZE = 1024 * 1024 * 304;
    int A_MEMORY_IN_NUM = 11;
    int A_MEMORY_LAST_OUT_HEAP_SIZE = 1024 * 1024 * 304;

    boolean PRINT_LOG = true;

    int DEST = 22439;
}
