package com.zbz;

import gnu.trove.list.array.TByteArrayList;
import gnu.trove.map.hash.TByteObjectHashMap;

import java.util.HashMap;
import java.util.LinkedHashMap;

/**
 * Created by bgk on 6/7/17.
 */
public class Binlog {
    private long primaryValue;

    private TByteObjectHashMap<byte[]> fields = new TByteObjectHashMap<>();

    public Binlog(long primaryValue) {
        this.primaryValue = primaryValue;
    }

    public void setPrimaryValue(long primaryValue) {
        this.primaryValue = primaryValue;
    }

    public long getPrimaryValue() {
        return primaryValue;
    }

    public void addField(byte fieldnameIndex, byte[] fieldValue) {
        fields.put(fieldnameIndex, fieldValue);
    }

    public TByteObjectHashMap<byte[]> getFields() {
        return fields;
    }
//    public String toSendString() {
//        StringBuilder sb = new StringBuilder(32);
//        sb.append(primaryValue).append("\t");
//        for (String fieldname : fields.keySet()) {
//            sb.append(fields.get(fieldname)).append("\t");
//        }
//        sb.setLength(sb.length() - 1);
//        return sb.toString();
//    }
}
