package com.zbz;

import java.util.LinkedHashMap;

/**
 * Created by bgk on 6/9/17.
 */
public class Table {
    private LinkedHashMap<String, Byte> fields = new LinkedHashMap<>();
    private String primaryKey = null;
    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void put(String fieldname, byte type) {
        fields.put(fieldname, type);
    }

    public LinkedHashMap<String, Byte> getFields() {
        return fields;
    }
}
