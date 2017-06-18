package com.zbz;

import gnu.trove.map.hash.TByteByteHashMap;

/**
 * Created by bgk on 6/18/17.
 */
public class DataConstans {
    public static final byte SEPARATOR = '|';
    public static final byte INNER_SEPARATOR = ':';
    public static final byte LF = '\n';

    public static final byte BINARY_PRE_SIZE = 15; // |mysql-bin.0000
    public static final byte OTHER_PRE_SIZE = 34; // 1496737946000|middleware3|student|
    public static final byte ID_SIZE = 8; // |id:1:1|
    public static final byte NULL_SIZE = 5; // NULL|
    public static final byte FIELD_TYPE_SIZE = 4; // 2:0|


    public static final byte FIELD_COUNT = 4;

    public static final byte DATABUF_CAPACITY = 64;

    public static final int HASHMAP_CAPACITY = 8388608;

}
