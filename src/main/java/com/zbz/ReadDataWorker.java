package com.zbz;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * Created by zwy on 17-6-8.
 */
public class ReadDataWorker implements Runnable {

    public static final int FILE_CNT = 3;

    private BinlogPool binlogPool;
    private BinlogReducer binlogReducer;
    private String dataHome;
    private String schema;
    private String table;

    public ReadDataWorker(BinlogPool binlogPool, String dataHome, String schema, String table) {
        this.binlogReducer = new BinlogReducer(schema, table);
        this.binlogPool = binlogPool;
        this.dataHome = dataHome;
        this.schema = schema;
        this.table = table;
    }

    private String getFilename(int i) {
        return dataHome + "/canal_0" + i;
    }

    public void run() {
        try {

            for(int i = 0; i < FILE_CNT; i++) {
                String filename = getFilename(i);
                BufferedReader reader = new BufferedReader(new FileReader(filename));
                String line;
                while ((line = reader.readLine()) != null) {
                    binlogReducer.reduce(line);
                    if (binlogReducer.isFull()) {
                        clearBinlogReducer();
                    }
//                System.out.println(line);
                }
                clearBinlogReducer();
                binlogPool.put(new Binlog());
                reader.close();
            }
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
