package io.openmessaging;

/**
 * Created by yanghuiwei on 2019-08-06
 */
public class PrimaryIndex {
    int[] t = new int[Const.INDEX_ELE_LENGTH];
    //t和a的内存块下标
    int[] a = new int[Const.INDEX_ELE_LENGTH];

    int[] pos = new int[Const.INDEX_ELE_LENGTH];


    int[] aMin = new int[Const.INDEX_ELE_LENGTH];
    int[] aMax = new int[Const.INDEX_ELE_LENGTH];
    int[] aSum = new int[Const.INDEX_ELE_LENGTH];


}
