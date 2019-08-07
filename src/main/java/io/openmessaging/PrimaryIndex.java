package io.openmessaging;

/**
 * Created by yanghuiwei on 2019-08-06
 */
public class PrimaryIndex {
    int[] tArr = new int[Const.INDEX_ELE_LENGTH];
    int[] tOffsetArr = new int[Const.INDEX_ELE_LENGTH];
    int[] aArr = new int[Const.INDEX_ELE_LENGTH];
    int[] aOffsetArr = new int[Const.INDEX_ELE_LENGTH];

    //t和a的内存块下标，t存低16位，a存高16位
    int[] taPosArr = new int[Const.INDEX_ELE_LENGTH];

    int[] aMinArr = new int[Const.INDEX_ELE_LENGTH];
    int[] aMaxArr = new int[Const.INDEX_ELE_LENGTH];
    long[] aSumArr = new long[Const.INDEX_ELE_LENGTH];
}
