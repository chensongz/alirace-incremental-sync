package com.zbz;

import java.util.HashMap;
import java.util.LinkedHashMap;

/**
 * Created by bgk on 6/7/17.
 */
public class Binlog {
    // 1 represent I, 2 represent U, 3 represent D
    public static final byte ID = 0;
    public static final byte I = 1;
    public static final byte U = 2;
    public static final byte D = 3;
    public static final byte DI = 4;

    private byte operation;

    private String primaryKey = null;

    private Long primaryOldValue;

    private Long primaryValue;

    private LinkedHashMap<String, String> fields = new LinkedHashMap<>();

    public void setOperation(byte operation) {
        this.operation = operation;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public void setPrimaryOldValue(String primaryOldValue) {
        if (primaryOldValue.equals("NULL")) {
            this.primaryOldValue = Long.MIN_VALUE + 1;
        } else {
            this.primaryOldValue = Long.parseLong(primaryOldValue);
        }
    }

    public void setPrimaryValue(Long primaryValue) {
        this.primaryValue = primaryValue;
    }

    public void setPrimaryValue(String primaryValue) {
        if (primaryValue.equals("NULL")) {
            this.primaryValue = Long.MIN_VALUE + 1;
        } else {
            this.primaryValue = Long.parseLong(primaryValue);
        }
    }

    public byte getOperation() {
        return operation;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public Long getPrimaryOldValue() {
        return primaryOldValue;
    }

    public Long getPrimaryValue() {
        return primaryValue;
    }

    public void addField(String fieldname, String fieldValue) {
        fields.put(fieldname, fieldValue);
    }

    public HashMap<String, String> getFields() {
        return fields;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(32);
        sb.append(operation).append("|");
        sb.append(primaryKey).append(":")
                .append(primaryOldValue).append(":")
                .append(primaryValue).append("|");
        for (String fieldname : fields.keySet()) {
            sb.append(fieldname).append(":")
                    .append(fields.get(fieldname)).append("|");
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    public byte[] toBytes() {
        return toString().getBytes();
    }

    public String toSendString() {
        StringBuilder sb = new StringBuilder(32);
        sb.append(primaryValue).append("\t");
        for (String fieldname : fields.keySet()) {
            sb.append(fields.get(fieldname)).append("\t");
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }
}
