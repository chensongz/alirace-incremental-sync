package com.zbz;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import java.nio.ByteBuffer;

/**
 * Created by zwy on 17-6-10.
 */
public class Record implements Comparable<Record>{

    private static final String SEPARATOR = "\t";

    private LinkedHashMap<String, String> fieldHashMap = new LinkedHashMap<>();
    private long primaryKeyVal;

    public Record() {
        primaryKeyVal = Long.MIN_VALUE;
    }

    public byte[] toBytes() {
        return toString().getBytes();
    }

    public void put(String fieldname, String value) {
        fieldHashMap.put(fieldname, value);
    }

    public long getPrimaryKeyValue() {
        return primaryKeyVal;
    }

    public void setPrimaryKeyValue(long val) {
        primaryKeyVal = val;
    }

    public LinkedHashMap<String, String> getFields() {
        return fieldHashMap;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        LinkedHashMap<String, String> fields = getFields();
        for (String value : fields.values()) {
            sb.append(value).append(SEPARATOR);
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    @Override
    public int compareTo(Record o) {
        long k1 = primaryKeyVal;
        long k2 = o.getPrimaryKeyValue();
        return k1 >= k2 ? 1 : -1;
    }
}
