package com.zbz;


public class Database {


    private static final Database database = new Database();
    public static Database getInstance() {
        return database;
    }

    private Table table = null;
    private BTree tree = null;

    public void init(Binlog binlog) {
        if (binlog.getOperation() != Binlog.I) return;
        //create table
        if (table == null) {
            table = new Table();
            table.put(binlog.getPrimaryKey(), Field.NUMERIC);
        }
    }

    public void insert(Binlog binlog) {
    }

    public void update(Binlog binlog) {
    }

    public void delete(Binlog binlog) {
    }
}
