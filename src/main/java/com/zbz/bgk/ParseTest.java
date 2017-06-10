package com.zbz.bgk;

import com.zbz.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by bgk on 6/9/17.
 */
public class ParseTest {
    public static void main(String[] args) throws IOException {
        BinlogPool binlogPool = BinlogPool.getInstance();

        String filename = "/home/zwy/work/test/canal.txt";
        Thread t1 = new Thread(new ReadDataWorker(binlogPool, filename, "", "student"));
        t1.start();

        Thread t2 = new Thread(new DatabaseWorker(binlogPool));
        t2.start();

    }
}
