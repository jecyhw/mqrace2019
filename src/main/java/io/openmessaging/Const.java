package io.openmessaging;

/**
 * Created by yanghuiwei on 2019-07-26
 */
public interface Const {
    String STORE_PATH = "/alidata1/race2019/data/";
    String MSG_FILE_SUFFIX = ".msg";
    String T_FILE_SUFFIX = ".t";
    String A_FILE_SUFFIX = ".a";

    int PRINT_MSG_INTERVAL = 1024 * 128;
    int MSG_BYTES = 34;
    int LONG_BYTES = 8;
    int MSG_COUNT = 34;
    int BUFFER_SIZE = MSG_COUNT * 4096; // 必须是8的倍数
    int MAX_LONG_CAPACITY = BUFFER_SIZE / LONG_BYTES;
    int MAX_MSG_CAPACITY = BUFFER_SIZE / MSG_BYTES;
}
