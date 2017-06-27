package com.reborn;

/**
 * Created by bgk on 6/27/17.
 */
public class Binlog {
    public int primaryValue;
    public int primaryOldValue;
    public byte operation;

    public byte[] fieldNames = new byte[DataConstants.FIELD_COUNT];
    public long[] fieldValues = new long[DataConstants.FIELD_COUNT];
}
