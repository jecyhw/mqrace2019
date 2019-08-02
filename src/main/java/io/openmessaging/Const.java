package io.openmessaging;

/**
 * Created by yanghuiwei on 2019-07-26
 */
public interface Const {
    String STORE_PATH = "./data/";
    String MSG_FILE_SUFFIX = ".msg";
    String T_FILE_SUFFIX = ".t";
    String A_FILE_SUFFIX = ".a";

    int PRINT_MSG_INTERVAL = 1024 * 128;
    int MSG_BYTES = 34;
    int LONG_BYTES = 4;
    int PUT_BUFFER_SIZE = 4 * 4096; // 16k写入
    int GET_BUFFER_SIZE = 1024 * 1024 * 4; //一次对多读取4m
    int MAX_LONG_CAPACITY = GET_BUFFER_SIZE / LONG_BYTES;
    int MAX_MSG_CAPACITY = GET_BUFFER_SIZE / MSG_BYTES;


    int MEMORY_BUFFER_SIZE = 1024 * 1024;
    boolean MEMORY_BUFFER_DIRECT = true;
}
