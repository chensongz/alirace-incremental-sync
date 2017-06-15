package com.zbz.bak;


import com.alibaba.middleware.race.sync.Constants;
import com.zbz.Binlog;
import com.zbz.HashIndex;
import com.zbz.Index;

import java.util.*;

public class Database {

    private static final Database database = new Database();
    public static Database getInstance() {
        return database;
    }

    private Table table = null;
    private Index index = null;
    private Persistence1 persistence = null;

    public void init(Binlog binlog) {
        if (binlog.getOperation() != Binlog.I) return;

        if (table == null) {
            table = new Table();
            table.init(binlog);
        }

        if(persistence == null) {
            persistence = new Persistence1(Constants.MIDDLE_HOME + "/database");
            persistence.init(table);
        }

        if(index == null) {
            index = new HashIndex();
        }

        insert(binlog);
    }

    public void insert(Binlog binlog) {
        Record record = parseFromBinlog(binlog);
        long offset = persistence.insert(record);
        index.insert(binlog.getPrimaryValue(), offset);
    }

    public void update(Binlog binlog) {
        long offset;
        if (binlog.getPrimaryValue() != binlog.getPrimaryOldValue()) {
            // if update primary key
            offset = index.getOffset(binlog.getPrimaryOldValue());
            index.delete(binlog.getPrimaryOldValue());
            index.insert(binlog.getPrimaryValue(), offset);
        } else {
            offset = index.getOffset(binlog.getPrimaryValue());
        }

        Record record = persistence.query(offset);
        Record newRecord = parseFromBinlog(binlog, record);

        persistence.update(newRecord, offset);
    }

    public void delete(Binlog binlog) {
        index.delete(binlog.getPrimaryValue());
    }

    public List<Record> query(long start, long end) {
        List<Long> offsets = new ArrayList<>((int)(end - start));
        for (long i = start + 1; i < end; i++) {
            long offset = index.getOffset(i);
            if(offset >= 0) {
                offsets.add(offset);
            }
        }
        Collections.sort(offsets);

        List<Record> queryList = new ArrayList<>((int)(end - start));
        for(long offset: offsets) {
            queryList.add(persistence.query(offset));
        }
        return queryList;
    }

    private Record parseFromBinlog(Binlog binlog) {

        Record record = new Record();
//        HashMap<String, Field> fields = binlog.getFields();
//        for(String field: table.getFields().keySet()) {
//            if (table.isPrimaryKey(field)) {
//                record.put(field, String.valueOf(binlog.getPrimaryValue()), true);
//            } else {
//                Field val = fields.get(field);
//                record.put(field, val == null ? "NULL" : val.getValue(), false);
//            }
//        }
        return record;
    }

    public Record parseFromBinlog(Binlog binlog, Record oldRecord) {
        Record newRecord = parseFromBinlog(binlog);
        Record retRecord = new Record();

        LinkedHashMap<String, String> oldFields = oldRecord.getFields();
        LinkedHashMap<String, String> newFields = newRecord.getFields();

        for (String field : oldFields.keySet()) {
            retRecord.put(field, oldFields.get(field), table.isPrimaryKey(field));
        }
        for (String field : newFields.keySet()) {
            if (!newFields.get(field).equals("NULL")) {
                retRecord.put(field, newFields.get(field), table.isPrimaryKey(field));
            }
        }
        return retRecord;
    }

    public void close() {
        persistence.close();
    }
}
