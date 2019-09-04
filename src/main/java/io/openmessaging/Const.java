package io.openmessaging;

/**
 * Created by yanghuiwei on 2019-07-26
 */
public interface Const {
    String STORE_PATH = "./data/";
    String MSG_FILE_SUFFIX = ".msg";
    String A_FILE_SUFFIX = ".a";
    String M_A_FILE_SUFFIX = ".ma";
    int GET_AVG_COUNT = 310000;

    int MSG_BYTES = 34;

    int PUT_BUFFER_SIZE = 1024 * 16; // 16k写入
    int LONG_BYTES = 8;


    //固定成一块
    int MEMORY_BUFFER_SIZE = 1024 * 1024 * 37;


    int INDEX_INTERVAL = 64 * 64;
    int INDEX_ELE_LENGTH = 42500;//需要和INDEX_INTERVAL一起调
    //INDEX_INTERVAL 为48，INDEX_ELE_LENGTH=3700000
    //INDEX_INTERVAL 为64，INDEX_ELE_LENGTH=2800000
    //INDEX_INTERVAL 128，INDEX_ELE_LENGTH=1400000

    int T_DECREASE = 3; //取值范围 0-3

    int MAX_GET_AT_SIZE = 8 * 10000;
    int MAX_GET_MESSAGE_SIZE = 7 * 10000;


    boolean PRINT_LOG = true;

    int[] T_INDEX_INTERVALS = new int[]{
            1024 * 25,
            1024 * 12 + 512

    };
    int MAX_T_INDEX_INTERVAL = 1024 * 24;
    int MAX_T_INDEX_LENGTH = 82500;

    int A_INDEX_INTERVAL = 128;
    int A_INDEX_LENGTH = MAX_T_INDEX_LENGTH * (MAX_T_INDEX_INTERVAL / A_INDEX_INTERVAL); //BLOCK_INDEX_SIZE的倍数

    int FILE_NUMS = 12;
    int T_MEMORY_SIZE = 1024 * 1024 * 252;

    int DEST = 31000;

    int GET_THREAD_NUM = 24;
    int FILE_STORE_MSG_NUM = MAX_T_INDEX_INTERVAL * 6000;
    int A_CACHE_BYTES1 = 1024 * 1024 * 1024;
    int A_CACHE_BYTES2 = 1024 * 1024 * 1024 + 1024 * 1024 * 500;
}
