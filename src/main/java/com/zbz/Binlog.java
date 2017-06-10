package com.zbz;

import java.util.HashMap;

/**
 * Created by bgk on 6/7/17.
 */
public class Binlog {
    // 1 represent I, 2 represent U, 3 represent D
    public static final byte I = 1;
    public static final byte U = 2;
    public static final byte D = 3;

    private byte operation;

    private String primaryKey;

    private long primaryOldValue;

    private long primaryValue;

    private HashMap<String, Field> fields = new HashMap<>();

    public void setOperation(byte operation) {
        this.operation = operation;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public void setPrimaryOldValue(String primaryOldValue) {
        if (primaryOldValue.equals("NULL")) {
            this.primaryOldValue = -1;
        } else {
            this.primaryOldValue = Long.parseLong(primaryOldValue);
        }

    }

    public void setPrimaryValue(String primaryValue) {
        if (primaryValue.equals("NULL")) {
            this.primaryValue = -1;
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

    public long getPrimaryOldValue() {
        return primaryOldValue;
    }

    public long getPrimaryValue() {
        return primaryValue;
    }

    public void addField(Field field) {
        fields.put(field.getName(), field);
    }

    public HashMap<String, Field> getFields() {
        return fields;
    }

    @Override
    public String toString() {
        String s = "";
        s = s + "operation:" + operation + " primaryvalue: " + primaryValue;
        for (Field field: fields.values()) {
            s += " " + field.getName() + "-" + field.getValue() + "\t";
        }
        return s;
    }
}
