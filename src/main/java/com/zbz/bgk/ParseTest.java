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
        BinlogReducer binlogReducer = new BinlogReducer("student");
        BinlogPool binlogPool = BinlogPool.getInstance();

        Thread t = new Thread(new DatabaseWorker(binlogPool));
        t.start();

        String filename = "/home/zwy/work/test/canal.txt";
        BufferedReader reader = new BufferedReader(new FileReader(filename));
        String line;
        while ((line = reader.readLine()) != null) {
            binlogReducer.reduce(line);
            if (binlogReducer.isFull()) {
                for (Binlog binlog : binlogReducer.getBinlogHashMap().values()) {
                    binlogPool.put(binlog);
                }
                binlogReducer.clearBinlogHashMap();
            }
        }
        reader.close();
        t.interrupt();
    }
}
