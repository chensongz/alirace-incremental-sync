package com.zbz;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Created by Victor on 2017/6/10.
 */
public class DatabaseWorker implements Runnable {
    private BinlogPool binlogPool;
    private SendPool sendPool;
    private long start;
    private long end;

    public DatabaseWorker(BinlogPool binlogPool, SendPool sendPool, long start, long end) {
        this.binlogPool = binlogPool;
        this.sendPool = sendPool;
        this.start = start;
        this.end = end;
    }

    @Override
    public void run() {
        long t1 = System.currentTimeMillis();

        Database database = Database.getInstance();
        boolean init = false;
        while (!Thread.currentThread().isInterrupted()) {
            Binlog binlog =  binlogPool.poll();
            if (binlog.getFields().size() <= 0 && binlog.getPrimaryKey() == null) {
                break;
            }
            byte op = binlog.getOperation();
            switch(op) {
                case Binlog.I:
                    if (!init) {
                        database.init(binlog);
                        init = true;
                    } else {
                        database.insert(binlog);
                    }
                    break;
                case Binlog.U:
                    database.update(binlog);
                    break;
                case Binlog.D:
                    database.delete(binlog);
                    break;
                default:
                    break;
            }
        }
        //database created, execute query
        System.out.println("Query result: ");
        List<Record> queryList = database.query(start, end);
        for(Record record: queryList) {
            System.out.println(record);
            sendPool.put(record);
        }

        long t2 = System.currentTimeMillis();
        System.out.println("time: " + (t2 - t1) / 1000 + " s");

    }
}
