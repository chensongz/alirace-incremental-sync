package com.zbz.bgk;

import com.alibaba.middleware.race.sync.Constants;
import com.zbz.BinlogPool;
import com.zbz.DatabaseWorker;
import com.zbz.ReadDataWorker;

import java.io.IOException;

/**
 * Created by bgk on 6/9/17.
 */
public class ParseTest {
    public static void main(String[] args) throws IOException {
        BinlogPool binlogPool = BinlogPool.getInstance();

        Thread t1 = new Thread(new ReadDataWorker(binlogPool, Constants.DATA_HOME, "", "student"));
        t1.start();

        Thread t2 = new Thread(new DatabaseWorker(binlogPool));
        t2.start();

    }
}
