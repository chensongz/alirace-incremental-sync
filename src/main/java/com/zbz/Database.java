package com.zbz;


public class Database {

    private static Database database = new Database();

    public Database getInstance() {
        return database;
    }

    private Table table;
    private BTree tree;

    public void init(Binlog binlog) {
        long pk = binlog.getPrimaryKey();
        tree = new BTree();
    }

    public void insert(Binlog binlog) {
        long pk = binlog.getPrimaryKey();
    }

    public void update(Binlog binlog) {
        long pk = binlog.getPrimaryKey();
    }

    public void delete(Binlog binlog) {
        long pk = binlog.getPrimaryKey();
    }
}
