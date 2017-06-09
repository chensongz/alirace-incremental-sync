package com.zbz;

/**
 * Created by bgk on 6/7/17.
 */
public class Binlog {

    public static byte INSERT = 1;
    public static byte UPDATE = 2;
    public static byte DELETE = 3;

    private String schema;
    private String table;
    private byte operation;
    private long primaryKey;


    private KeyValue keyValue = new KeyValue();

    public Binlog(String schema, String table, String operation) {
        this.schema = schema;
        this.table = table;
        setOperation(operation);
    }

    public Binlog putKeyValue(String key, long value) {
        keyValue.put(key, value);
        return this;
    }

    public Binlog putKeyValue(String key, String value) {
        keyValue.put(key, value);
        return this;
    }

    public void setOperation(String operation) {
        switch (operation) {
            case "I":
                this.operation = Binlog.INSERT;
                break;
            case "U":
                this.operation = Binlog.UPDATE;
                break;
            case "D":
                this.operation = Binlog.DELETE;
                break;
            default:
                break;
        }
    }

    public byte getOperation() {
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

    public long getPrimaryKey() {
        return this.primaryKey;
    }
}
