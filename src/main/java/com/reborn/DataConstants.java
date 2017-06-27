package com.reborn;

/**
 * Created by bgk on 6/27/17.
 */
public class DataConstants {
    public static final int MAX_MESSAGE_SIZE = 256;
    public static final int MIN_MESSAGE_SIZE = 64;
    public static final int READ_MESSAGE_COUNT = 32;
    public static final int READ_BUFFER_SIZE = MAX_MESSAGE_SIZE*READ_MESSAGE_COUNT;
    public static final int MAX_MESSAGE_COUNT = READ_BUFFER_SIZE / MIN_MESSAGE_SIZE;

    public static final int RINGBUFFER_SIZE = 1 << 25; //must larger than READ_BUFFER_SIZE

    public static final int PARSER_COUNT = 2;




}
