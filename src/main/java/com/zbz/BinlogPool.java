package com.zbz;

/**
 * Created by zwy on 17-6-9.
 */
public class BinlogPool {
    private static final BinlogPool binlogPool = new BinlogPool();

    public static BinlogPool getInstance() {
        return binlogPool;
    }

    public void put(Binlog binLog) {
        //synchronize
    }

    public Binlog poll() {
        //synchronize
        return null;
    }
}
