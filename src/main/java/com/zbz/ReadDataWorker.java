package com.zbz;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * Created by zwy on 17-6-8.
 */
public class ReadDataWorker implements Runnable {

    private BinlogPool binlogPool;
    private BinlogReducer binlogReducer;
    private String dataHome;
    private String schema;
    private String table;

    public ReadDataWorker(BinlogPool binlogPool, String dataHome, String schema, String table) {
        this.binlogReducer = new BinlogReducer(table);
        this.binlogPool = binlogPool;
        this.dataHome = dataHome;
        this.schema = schema;
        this.table = table;
    }

    public void run() {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(dataHome));
            String line;
            while ((line = reader.readLine()) != null) {
                binlogReducer.reduce(line);
                if (binlogReducer.isFull()) {
                    clearBinlogReducer();
                }
                System.out.println(line);
            }
            clearBinlogReducer();
            binlogPool.put(new Binlog());
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void clearBinlogReducer() {
        for (Binlog binlog : binlogReducer.getBinlogHashMap().values()) {
            binlogPool.put(binlog);
        }
        binlogReducer.clearBinlogHashMap();
    }

}
