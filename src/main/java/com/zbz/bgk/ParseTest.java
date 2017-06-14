package com.zbz.bgk;

import com.alibaba.middleware.race.sync.Constants;
//import com.zbz.BinlogPool;
import com.zbz.Binlog;
import com.zbz.BinlogFactory;
import com.zbz.Index;
import com.zbz.zwy.Persistence;
//import com.zbz.SendPool;

import java.io.IOException;

/**
 * Created by bgk on 6/9/17.
 */
public class ParseTest {
    public static void main(String[] args) throws IOException {
//        BinlogPool binlogPool = BinlogPool.getInstance();
//        SendPool sendPool = SendPool.getInstance();
        long start = -2;
        long end = 800;
//
//        Thread t1 = new Thread(new ReadDataWorker(binlogPool, Constants.DATA_HOME, "", "student"));
//        t1.start();
//
//        Thread t2 = new Thread(new DatabaseWorker(binlogPool, sendPool, start, end));
//        t2.start();
        com.zbz.bgk.ReadDataWorker readDataWorker = new com.zbz.bgk.ReadDataWorker("middleware3", "student",
                "/home/zwy/work/test/canal_01", "/home/zwy/work/middlewareTester/middle/database");
        readDataWorker.compute();
        Index index = readDataWorker.getIndex();
        com.zbz.zwy.Persistence persistence = readDataWorker.getPersistence();
        for (long value = start; value < end; value++) {
            long offset = index.getOffset(value);
            if (offset >= 0) {
                String binlogLine = new String(persistence.read(offset));
                Binlog binlog = BinlogFactory.parse(binlogLine);
                System.out.println("result :" + binlog);
            }

        }
    }
}
