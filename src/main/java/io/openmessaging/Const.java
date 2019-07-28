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
    int MSG_BYTES = 50;
    int LONG_BYTES = 8;
    int BUFFER_SIZE = 50 * 4096;
}
