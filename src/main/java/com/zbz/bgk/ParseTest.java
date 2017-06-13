package com.zbz.bgk;

import com.alibaba.middleware.race.sync.Constants;
//import com.zbz.BinlogPool;
import com.zbz.DatabaseWorker;
import com.zbz.ReadDataWorker;
//import com.zbz.SendPool;

import java.io.IOException;

/**
 * Created by bgk on 6/9/17.
 */
public class ParseTest {
    public static void main(String[] args) throws IOException {
//        BinlogPool binlogPool = BinlogPool.getInstance();
//        SendPool sendPool = SendPool.getInstance();
//        long start = 550;
//        long end = 800;
//
//        Thread t1 = new Thread(new ReadDataWorker(binlogPool, Constants.DATA_HOME, "", "student"));
//        t1.start();
//
//        Thread t2 = new Thread(new DatabaseWorker(binlogPool, sendPool, start, end));
//        t2.start();
        com.zbz.bgk.ReadDataWorker readDataWorker = new com.zbz.bgk.ReadDataWorker("middleware3", "student",
                "/home/zwy/work/test/canal.txt", "/home/zwy/work/middlewareTester/middle/database");
        readDataWorker.compute();
    }
}
