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
    int MAX_MSG_CAPACITY = GET_BUFFER_SIZE / MSG_BYTES;
    int LONG_BYTES = 8;
    int MAX_LONG_CAPACITY = GET_BUFFER_SIZE / LONG_BYTES;


    //固定成一块
    int MEMORY_BUFFER_SIZE = 1024 * 1024 * 50;

    int INDEX_INTERVAL = 64;
    int INDEX_ELE_LENGTH = 3700000;//需要和INDEX_INTERVAL一起调
    //INDEX_INTERVAL 为48，INDEX_ELE_LENGTH=3700000
    //INDEX_INTERVAL 为64，INDEX_ELE_LENGTH=2800000
    //INDEX_INTERVAL 128，INDEX_ELE_LENGTH=1400000

    int A_DECREASE = 1;
    int T_DECREASE = 3; //取值范围 0-3

    int MAX_GET_AT_SIZE = 10 * 10000;
    int MAX_GET_MESSAGE_SIZE = 7 * 10000;

    int COMPRESS_MSG_SIZE = INDEX_INTERVAL * Const.MSG_BYTES;

    boolean PRINT_LOG = true;

    int DEST = 27000;
}
