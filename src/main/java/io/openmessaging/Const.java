package io.openmessaging;

/**
 * Created by yanghuiwei on 2019-07-26
 */
public interface Const {
    String STORE_PATH = "./data/";
    String MSG_FILE_SUFFIX = ".msg";
    int GET_AVG_COUNT = 31000;

    int MSG_BYTES = 34;

    int PUT_BUFFER_SIZE = 4 * 4096; // 16k写入
    int GET_BUFFER_SIZE = 1024 * 1024 * 4; //一次对多读取4m
    int MAX_MSG_CAPACITY = GET_BUFFER_SIZE / MSG_BYTES;

    int MEMORY_BUFFER_SIZE = 1024 * 1024;

    int INDEX_INTERVAL = 128;
    int INDEX_ELE_LENGTH = 1400000;//需要和INDEX_INTERVAL一起调
    //INDEX_INTERVAL 为64，INDEX_ELE_LENGTH=2800000

    int A_DECREASE = 1;

    int MAX_GET_MSG_SIZE = 50 * 10000;

    boolean PRINT_LOG = true;
    int PUT_THREAD_SIZE = 12;
}
