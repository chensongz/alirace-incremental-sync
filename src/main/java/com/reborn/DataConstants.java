package com.reborn;

/**
 * Created by bgk on 6/27/17.
 */
public class DataConstants {

    public static final int MAX_MESSAGE_SIZE = 256;
    public static final int MIN_MESSAGE_SIZE = 64;

    public static final int FIELD_COUNT = 5;
    public static final int PARSER_COUNT = 6;

    public static final int HASHMAP_CAPACITY = 8388608;

    public static final byte SEPARATOR = '|';

    public static final byte BINARY_PRE_SIZE = 20; // |mysql-bin.0000
    public static final byte OTHER_PRE_SIZE = 34; // 1496737946000|middleware3|student|
    public static final byte ID_SIZE = 8; // |id:1:1|
    public static final byte NULL_SIZE = 5; // NULL|
    public static final byte FIELD_TYPE_SIZE = 4; // 2:0|
    public static final byte DELETE_SKIP_COUNT = 106; //
//    public static final byte DELETE_SKIP_COUNT = 87; // for local test

    public static final int MAPSIZE = 1024 * 1024 * 8;
    public static final int MAPPED_BUFFER_CAPACITY = 32;
    public static final int MAX_MESSAGE_COUNT = MAPSIZE / MIN_MESSAGE_SIZE;


}
