package com.zbz;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * Created by zwy on 17-6-8.
 */
public class ReadDataWorker implements Runnable {

    private BinlogPool binlogPool;
    private String dataHome;
    private String schema;
    private String table;

    public ReadDataWorker(BinlogPool binlogPool, String dataHome, String schema, String table) {
        this.binlogPool = binlogPool;
        this.dataHome = dataHome;
        this.schema = schema;
        this.table = table;
    }

    public void run() {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(dataHome));
            String line;
            int i = 0;
            while ((line = reader.readLine()) != null) {
//                System.out.println(line);
                Binlog log = parse(line);
                binlogPool.put(log);
                if (i++ >= 500) break;
            }

            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Binlog parse(String line) {
        //filter by `schema` and `table` to avoid newing extra `Binlog`
        return null;
    }
}
