package com.zbz;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import java.nio.ByteBuffer;

/**
 * Created by zwy on 17-6-10.
 */
public class Record {

    private static final String SEPARATOR = "\t";

    private LinkedHashMap<String, String> fieldHashMap = new LinkedHashMap<>();

    public Record() {
    }

    public ByteBuffer toBytes() {
        ByteBuffer byteBuffer = ByteBuffer.wrap(toString().getBytes());
        return byteBuffer;
    }

    public static Record parseFromBytes(ByteBuffer recordBytes, Table table) {
        recordBytes.flip();
        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        while(recordBytes.remaining() > 0) {
            byte curr = recordBytes.get();
            if(curr != (byte)0) {
                bao.write(curr);
            }
        }
        String recordString = bao.toString();
//        System.out.println("record string:" + recordString);
        return parse(recordString, table);
    }

    public static Record parseFromBinlog(Binlog binlog, Table table) {
        Record record = new Record();
        HashMap<String, Field> fields = binlog.getFields();
        for(String field: table.getFields().keySet()) {
            if (field.equals(binlog.getPrimaryKey())) {
                record.put(field, String.valueOf(binlog.getPrimaryValue()));
            } else {
                Field val = fields.get(field);
                record.put(field, val == null ? "NULL" : val.getValue());
            }
        }
        return record;
    }

    public void put(String fieldname, String value) {
        fieldHashMap.put(fieldname, value);
    }

    public LinkedHashMap<String, String> getFields() {
        return fieldHashMap;
    }

    public static Record parseFromBinlog(Binlog binlog, Table table, Record record) {
        Record newRecord = parseFromBinlog(binlog, table);
        Record retRecord = new Record();
        LinkedHashMap<String, String> oldFields = record.getFields();
        LinkedHashMap<String, String> newFields = newRecord.getFields();
        System.out.println("old record:" + record);
        System.out.println("new record:" + newRecord);
        for (String fieldname : oldFields.keySet()) {
            retRecord.put(fieldname, oldFields.get(fieldname));
        }
        for (String fieldname : newFields.keySet()) {
            if (!newFields.get(fieldname).equals("NULL")) {
                // if update binlog not includes all fields
//                System.out.println("new record put " + fieldname + ":" + newFields.get(fieldname));
                retRecord.put(fieldname, newFields.get(fieldname));
            }
        }
        return retRecord;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        LinkedHashMap<String, String> fields = getFields();
        for (String value : fields.values()) {
            sb.append(value).append(SEPARATOR);
        }
        return sb.toString();
    }

    public static Record parse(String str, Table table) {
        System.out.println("parse str:" + str);
        String[] vals = str.split(SEPARATOR);
        LinkedHashMap<String, Byte> fields = table.getFields();
        Record ret = new Record();
        int i = 0;
        for(String field: fields.keySet()) {
            ret.put(field, vals[i++]);
        }
        return ret;
    }
}
