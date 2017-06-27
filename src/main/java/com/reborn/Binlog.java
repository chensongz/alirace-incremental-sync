package com.reborn;

/**
 * Created by bgk on 6/27/17.
 */
public class Binlog {
    public int primaryValue;
    public int primaryOldValue;
    public byte operation;
    public byte fieldLength; // update or insert how many fields
    public byte[] fieldNames = new byte[DataConstants.FIELD_COUNT];
    public long[] fieldValues = new long[DataConstants.FIELD_COUNT];

    public void reset() {
        fieldLength = 0;
    }

    public void put(byte fieldName, long fieldValue) {
        fieldNames[fieldLength] = fieldName;
        fieldValues[fieldLength] = fieldValue;
        fieldLength++;
    }
}
