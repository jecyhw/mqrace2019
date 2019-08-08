package io.openmessaging;

/**
 * Created by yanghuiwei on 2019-08-06
 */
public class PrimaryIndex {
    int[] tArr = new int[Const.INDEX_ELE_LENGTH];
    int[] aArr = new int[Const.INDEX_ELE_LENGTH];

    int[] offsetArr = new int[Const.INDEX_ELE_LENGTH];

    int[] aMinArr = new int[Const.INDEX_ELE_LENGTH];
    int[] aMaxArr = new int[Const.INDEX_ELE_LENGTH];
    long[] aSumArr = new long[Const.INDEX_ELE_LENGTH];
}
