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

    private LinkedHashMap<String, String> fieldHashMap = new LinkedHashMap<>();

    public Record() {
    }

    public ByteBuffer toBytes() {
        ByteBuffer byteBuffer = ByteBuffer.wrap(toString().getBytes());
        return byteBuffer;
    }

    public static Record parseFromBytes(ByteBuffer recordBytes, Table table) {
        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        while(recordBytes.remaining() > 0) {
            byte curr = recordBytes.get();
            if(curr != (byte)0) {
                bao.write(curr);
            }
        }
        String recordString = bao.toString();
        return parse(recordString, table);
    }

    public static Record parseFromBinlog(Binlog binlog, Table table) {
        Record record = new Record();
        HashMap<String, Field> fields = binlog.getFields();
        for(String field: table.getFields().keySet()) {
            Field val = fields.get(field);
            record.put(field, val == null ? "" : val.getValue());
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

        for (String fieldname : oldFields.keySet()) {
            retRecord.put(fieldname, oldFields.get(fieldname));
        }
        for (String fieldname : newFields.keySet()) {
            retRecord.put(fieldname, newFields.get(fieldname));
        }
        return record;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        LinkedHashMap<String, String> fields = getFields();
        int i = 0;
        for (String value : fields.values()) {
            sb.append(value).append("\t");
            if(i++ == 0) sb.append(";");
        }
//        System.out.println(sb.toString());
        return sb.toString();
    }

    public static Record parse(String str, Table table) {
        String[] vals = str.split("\t");
        LinkedHashMap<String, Byte> fields = table.getFields();
        Record ret = new Record();
        int i = 0;
        for(String field: fields.keySet()) {
            ret.put(field, vals[i++]);
        }
        return ret;
    }
}
