package com.zbz;

/**
 * Created by Victor on 2017/6/10.
 */
public class PersistenceThread implements Runnable {
    private BinlogPool binlogPool;

    public PersistenceThread(BinlogPool binlogPool) {
        this.binlogPool = binlogPool;
    }

    @Override
    public void run() {
        Database database = Database.getInstance();
        boolean init = false;
        while (!Thread.currentThread().isInterrupted()) {
            Binlog binlog =  binlogPool.poll();
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
    }
}
