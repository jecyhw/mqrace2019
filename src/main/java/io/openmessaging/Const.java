package io.openmessaging;

/**
 * Created by yanghuiwei on 2019-07-26
 */
public interface Const {
    String STORE_PATH = "../data/";
    String MSG_FILE_SUFFIX = ".msg";
    String A_FILE_SUFFIX = ".a";
    String M_A_FILE_SUFFIX = ".ma";
    String M_A_SORT_FILE_SUFFIX = ".maSort";
    int GET_AVG_COUNT = 310000;

    int MSG_BYTES = 34;

    int PUT_BUFFER_SIZE = 1024 * 1024; // 16k写入
    int M_PUT_BUFFER_SIZE = 1024 * 64; // 16k写入
    int LONG_BYTES = 8;


    //固定成一块
    int MEMORY_BUFFER_SIZE = 1024 * 1024 * 42;


    int INDEX_INTERVAL = 64 * 32;
    int INDEX_ELE_LENGTH = 90000;//需要和INDEX_INTERVAL一起调
    //INDEX_INTERVAL 为48，INDEX_ELE_LENGTH=3700000
    //INDEX_INTERVAL 为64，INDEX_ELE_LENGTH=2800000
    //INDEX_INTERVAL 128，INDEX_ELE_LENGTH=1400000

    int T_DECREASE = 3; //取值范围 0-3

    int MAX_GET_AT_SIZE = 8 * 10000;
    int MAX_GET_MESSAGE_SIZE = 7 * 10000;


    boolean PRINT_LOG = true;

    int MERGE_T_TIME = 2;
    int MERGE_T_INDEX_INTERVAL = 1024 * 4 * MERGE_T_TIME;
    int MERGE_T_INDEX_LENGTH = 31000 * 16 / MERGE_T_TIME;
    int A_INDEX_INTERVAL = 16;
    int A_INDEX_LENGTH = MERGE_T_INDEX_LENGTH * (MERGE_T_INDEX_INTERVAL / A_INDEX_INTERVAL); //BLOCK_INDEX_SIZE的倍数

    int FILE_NUMS = 32;
    int T_MEMORY_SIZE = 1024 * 1024 * 252;

    int DEST = 26000;

    int getThreadNums = 12;
}
