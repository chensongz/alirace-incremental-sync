package com.zbz.bgk;

/**
 * Created by bgk on 6/17/17.
 */

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.Server;
import com.zbz.Binlog;
import com.zbz.BinlogReducer;
import com.zbz.Pool;
import gnu.trove.map.hash.TLongObjectHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by zwy on 17-6-14.
 */
public class TestReducer implements Runnable {


    private String schema;
    private String table;
    private long start;
    private long end;
    private Pool<String> sendPool;
    private BinlogReducer binlogReducer;

    public TestReducer(String schema, String table, long start, long end, Pool<String> sendPool) {
        this.schema = schema;
        this.table = table;
        this.start = start;
        this.end = end;
        this.sendPool = sendPool;
        binlogReducer = new BinlogReducer(schema, table);
    }

    @Override
    public void run() {
        for (int i = 0; i < Constants.DATA_FILE_NUM; i++) {
            String dataFileName = Constants.getDataFile(i);
            try {
                reduceDataFile(dataFileName);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        TLongObjectHashMap<Binlog> binlogTLongObjectHashMap = binlogReducer.getBinlogHashMap();
        for (long key = start + 1; key < end; key ++) {
            Binlog binlog = binlogTLongObjectHashMap.get(key);
            if (binlog != null) {
                System.out.println(binlog.toSendString());
                sendPool.put(binlog.toSendString());
            }
        }
    }





    private void reduceDataFile(String filename) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(filename));
        String line;
        long t1 = System.currentTimeMillis();
        while ((line = reader.readLine()) != null) {
            binlogReducer.reduce(line);
        }
        reader.close();
        long t2 = System.currentTimeMillis();
        Logger logger = LoggerFactory.getLogger(Server.class);
        logger.info(filename + " reduced... cost time:" + (t2-t1));
    }

}

