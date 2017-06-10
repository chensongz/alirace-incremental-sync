package com.zbz;


import com.alibaba.middleware.race.sync.Constants;

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
                    table.setPkIdx(pkIdx);
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
    }

    public void insert(Binlog binlog) {
        Record record = Record.parseFromBinlog(binlog, table);
        long offset = persistence.insert(record);
        index.insert(binlog.getPrimaryValue(), offset);
    }

    public void update(Binlog binlog) {
        long offset = index.getOffset(binlog.getPrimaryValue());
        Record record = persistence.query(offset);
        Record newRecord = Record.parseFromBinlog(binlog, table, record);

        persistence.update(newRecord, offset);
    }

    public void delete(Binlog binlog) {
        index.delete(binlog.getPrimaryValue());
    }
}
