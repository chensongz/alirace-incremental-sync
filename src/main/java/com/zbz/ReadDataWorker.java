package com.zbz;

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.Server;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by zwy on 17-6-8.
 */
public class ReadDataWorker implements Runnable {

    private Pool<Binlog> binlogPool;
    private BinlogReducer binlogReducer;

    public ReadDataWorker(Pool<Binlog> binlogPool, String schema, String table) {
        this.binlogReducer = new BinlogReducer(schema, table);
        this.binlogPool = binlogPool;
    }

    public void run() {
            long t1 = System.currentTimeMillis();

            try {
                for(int i = 0; i < Constants.DATA_FILE_NUM; i++) {
                    String filename = Constants.getDataFile(i);
                    System.out.println("reading data file: " + filename + " ......");
                    reduceDataFile(filename);
                }
                binlogPool.put(new Binlog());
            } catch (IOException e) {
                e.printStackTrace();
            }

            long t2 = System.currentTimeMillis();
            String p = "Server readDataWorker: " + (t2 - t1) + "ms";
            System.out.println(p);
            LoggerFactory.getLogger(Server.class).info(p);
    }

    private void reduceDataFile(String filename) throws IOException{
        BufferedReader reader = new BufferedReader(new FileReader(filename));
        String line;

        while ((line = reader.readLine()) != null) {
            binlogReducer.reduce(line);
            if (binlogReducer.isFull()) {
                clearBinlogReducer();
            }
        }
        clearBinlogReducer();

        reader.close();
        System.out.println(filename + " reduced...");
    }

    private void clearBinlogReducer() {
        for (Binlog binlog : binlogReducer.getBinlogHashMap().values()) {
            binlogPool.put(binlog);
        }
        binlogReducer.clearBinlogHashMap();
    }

}
