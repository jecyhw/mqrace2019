package io.openmessaging;

/**
 * Created by yanghuiwei on 2019-07-26
 */
public interface Const {
    String STORE_PATH = "./data/";
    String MSG_FILE_SUFFIX = ".msg";

    int PRINT_MSG_INTERVAL = 1024 * 128 * 128 * 16;
    int MSG_BYTES = 34;
    int LONG_BYTES = 4;

    int PUT_BUFFER_SIZE = 4 * 4096; // 16k写入
    int GET_BUFFER_SIZE = 1024 * 1024 * 4; //一次对多读取4m
    int MAX_MSG_CAPACITY = GET_BUFFER_SIZE / MSG_BYTES;

    int MEMORY_BUFFER_SIZE = 1024 * 1024;

    int INDEX_BUFFER_SIZE = 1008 * 1024; //必须是INDEX_ELE_SIZE的倍数
    int INDEX_INTERVAL = 128;
    int INDEX_ELE_SIZE = 36;
    int INDEX_ELE_LENGTH = INDEX_BUFFER_SIZE / INDEX_ELE_SIZE;

    int A_BYTES = 2;

    boolean PRINT_LOG = true;

}
