package com.zbz.bak;

import com.alibaba.middleware.race.sync.Server;
import com.zbz.Binlog;
import com.zbz.Pool;
import com.zbz.zwy.TimeTester;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * Created by Victor on 2017/6/10.
 */
public class DatabaseWorker implements Runnable {
    private Pool<Binlog> binlogPool;
    private Pool<Record> sendPool;
    private long start;
    private long end;

    public DatabaseWorker(Pool<Binlog> binlogPool, Pool<Record> sendPool, long start, long end) {
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
        System.out.println("Start quering: " + start + "-" + end + " ......");
        List<Record> queryList = database.query(start, end);
        database.close();

        Collections.sort(queryList);

        for(Record record: queryList) {
//            System.out.println("Query result: " + record.toString());
            sendPool.put(record);
        }
        sendPool.put(new Record());

        long t2 = System.currentTimeMillis();
        TimeTester timer = TimeTester.getInstance();
        timer.setT2(System.currentTimeMillis());
        String p = "Server databaseWorker: " + (t2 - t1) + "ms";
        System.out.println(p);
        LoggerFactory.getLogger(Server.class).info(p);
        System.out.println("Two worker: " + (timer.getT2() - timer.getT1()) + "ms");
    }
}