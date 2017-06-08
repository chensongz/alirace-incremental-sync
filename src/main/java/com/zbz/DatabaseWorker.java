package com.zbz;

/**
 * Created by zwy on 17-6-9.
 */
public class DatabaseWorker implements Runnable{

    private BinlogPool binlogPool;

    public DatabaseWorker(BinlogPool binlogPool) {
        this.binlogPool = binlogPool;
    }

    public void run() {
        //TODO when to poll binlog
        while (true) {
            Binlog binlog = binlogPool.poll();
        }
    }
}
