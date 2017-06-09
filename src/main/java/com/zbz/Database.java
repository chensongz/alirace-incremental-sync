package com.zbz;

/**
 * Created by bgk on 6/9/17.
 */
public class Database {

    private static final Database database = new Database();

    public Database getInstance() {
        return database;
    }

    private Table table;
    private BTree tree;

    public void init(Binlog binlog) {

    }

    public void insert(Binlog binlog) {

    }

    public void update(Binlog binlog) {

    }

    public void delete(Binlog binlog) {

    }
}
