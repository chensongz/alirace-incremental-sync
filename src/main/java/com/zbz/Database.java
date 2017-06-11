package com.zbz;


import com.alibaba.middleware.race.sync.Constants;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Database {


    private static final Database database = new Database();
    public static Database getInstance() {
        return database;
    }

    private Table table = null;
    private Persistence persistence = null;
    private Index index = null;

    public void init(Binlog binlog) {
        if (binlog.getOperation() != Binlog.I) return;
        //create table
        if (table == null) {
            int pkIdx = binlog.getPrimaryKeyIndex();
            table = new Table();
            Map<String, Field> fields = binlog.getFields();

            int i = 0;
            for (Field field : fields.values()) {
                if (i++ == pkIdx) {
                    table.put(binlog.getPrimaryKey(), Field.NUMERIC);
                }
                table.put(field.getName(), field.getType());
            }
        }

        if(persistence == null) {
            persistence = new Persistence(Constants.MIDDLE_HOME + "/database");
            persistence.init(table);
        }

        if(index == null) {
            index = new HashIndex();
        }

        insert(binlog);
    }

    public void insert(Binlog binlog) {
        Record record = Record.parseFromBinlog(binlog, table);
        long offset = persistence.insert(record);
        index.insert(binlog.getPrimaryValue(), offset);
//        if(binlog.getPrimaryValue() > 600 && binlog.getPrimaryValue() < 700) {
//            System.out.println("pk: " + binlog.getPrimaryValue() + " offset: " + offset);
//        }
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
//        System.out.println("offset:" + offset);

        Record record = persistence.query(offset);
        Record newRecord = Record.parseFromBinlog(binlog, table, record);
        persistence.update(newRecord, offset);
    }

    public void delete(Binlog binlog) {
        index.delete(binlog.getPrimaryValue());
    }

    public List<Record> query(long start, long end) {
        System.out.println("query range: "  + start + "-" + end);
        List<Long> offsets = new ArrayList<>((int)(end - start));
        for (long i = start + 1; i < end; i++) {
            long offset = index.getOffset(i);
//            System.out.println("current query: " + i + " offset: " + offset);
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
}
