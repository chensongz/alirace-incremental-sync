package com.zbz;

/**
 * Created by bgk on 6/7/17.
 */
public class Binlog {
    private String schema;
    private String table;
    private String operation;

    private KeyValue keyValue = new KeyValue();

    public Binlog(String schema, String table, String operation) {
        this.schema = schema;
        this.table = table;
        this.operation = operation;
    }

    public Binlog putKeyValue(String key, long value) {
        keyValue.put(key, value);
        return this;
    }

    public Binlog putKeyValue(String key, String value) {
        keyValue.put(key, value);
        return this;
    }

    public Binlog setOperation(String operation) {
        this.operation = operation;
        return this;
    }

    public String getOperation() {
        return this.operation;
    }

    public String getSchema() {
        return this.schema;
    }

    public String getTable() {
        return this.table;
    }

    public void addField(Field field) {
        keyValue.put(field.getFieldname(), field);
    }

    public Field getPrimaryKey() {
        for (Object field : keyValue.values()) {
            if (((Field)field).isPrimaryKey()) {
                return (Field)field;
            }
        }
        return null;
    }


}
