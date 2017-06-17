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
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

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
        Logger logger = LoggerFactory.getLogger(Server.class);
        logger.info("TestReducer run start");
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < Constants.DATA_FILE_NUM; i++) {
            try {
                reduceDataFile(Constants.getDataFile(i));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        long t2 = System.currentTimeMillis();
        logger.info("reduce all cost: " + (t2 - t1) + " ms");
        logger.info("insert count: " + binlogReducer.insertCount);
        logger.info("update count: " + binlogReducer.updateCount);
        logger.info("delete count: " + binlogReducer.deleteCount);
        TLongObjectHashMap<Binlog> binlogTLongObjectHashMap = binlogReducer.getBinlogHashMap();
        logger.info("TestReducer start sendPool");
        int sendCount = 0;
        for (long key = start + 1; key < end; key++) {
            Binlog binlog = binlogTLongObjectHashMap.get(key);
            if (binlog != null) {
                sendPool.put(binlog.toSendString());
                sendCount++;
            }
        }
        sendPool.put("NULL");
        logger.info("send binlog count: " + sendCount);
    }

    private void reduceDataFile(String filename) throws IOException {
        Logger logger = LoggerFactory.getLogger(Server.class);
        File file = new File(filename);
        BufferedReader reader = new BufferedReader(new FileReader(file));
        logger.info(filename + " size: " + file.length());
        String line;
        long t1 = System.currentTimeMillis();
        while ((line = reader.readLine()) != null) {
            binlogReducer.reduce(line);
        }
        reader.close();
        long t2 = System.currentTimeMillis();
//        logger.info(filename + " reduce cost time: " + (t2 - t1) + " ms");
        logger.info(filename + " readline and parse cost time: " + (t2 - t1) + " ms");
        logger.info(filename + " parse cost time: " + binlogReducer.getParseBinlogTime() + " ms");
    }

}

