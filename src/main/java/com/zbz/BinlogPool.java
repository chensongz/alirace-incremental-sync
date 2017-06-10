package com.zbz;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by zwy on 17-6-9.
 */
public class BinlogPool {
    private static final BinlogPool binlogPool = new BinlogPool();

    public static BinlogPool getInstance() {
        return binlogPool;
    }

    private BlockingQueue<Binlog> binlogs = new LinkedBlockingQueue<>(5000);

    public void put(Binlog binLog) {
        //synchronize
        try {
            binlogs.put(binLog);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public Binlog poll() {
        //synchronize
        try {
            return binlogs.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

}
