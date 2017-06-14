package com.zbz.bak;

import com.zbz.Binlog;
import com.zbz.Field;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by bgk on 6/9/17.
 */
public class Table {
    private LinkedHashMap<String, Byte> fields = new LinkedHashMap<>();
    private String primaryKey = null;

    public void init(Binlog binlog) {
        //TODO
//        int pkIdx = binlog.getPrimaryKeyIndex();
        Map<String, Field> fields = binlog.getFields();

        int i = 0;
        for (Field field : fields.values()) {
//            if (i++ == pkIdx) {
//                primaryKey = binlog.getPrimaryKey();
//                this.fields.put(primaryKey, Field.NUMERIC);
//            }
            this.fields.put(field.getName(), field.getType());
        }
    }

    public boolean isPrimaryKey(String field) {
        return primaryKey.equals(field);
    }

    public LinkedHashMap<String, Byte> getFields() {
        return fields;
    }
}
