package com.zbz;

import java.util.LinkedHashMap;

/**
 * Created by bgk on 6/9/17.
 */
public class Table {
    private LinkedHashMap<String, Byte> fields = new LinkedHashMap<>();
    private int pkIdx;

    public void put(String fieldname, byte type) {
        fields.put(fieldname, type);
    }

    public void setPkIdx(int pkIdx) {
        this.pkIdx = pkIdx;
    }

    public int getPkIdx() {
        return pkIdx;
    }

    public LinkedHashMap<String, Byte> getFields() {
        return fields;
    }
}
