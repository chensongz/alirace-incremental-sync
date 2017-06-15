package com.zbz;

import com.zbz.zwy.TimeTester;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.middleware.race.sync.Server;

/**
 * Created by bgk on 6/13/17.
 */
public class InnerFileReducer {
    private BinlogReducer binlogReducer;
    private String srcFilename;
    private Index index;
    private Persistence persistence;
    public InnerFileReducer(String schema, String table, String srcFilename, String dstFilename) {
        this.binlogReducer = new BinlogReducer(schema, table);
        this.srcFilename = srcFilename;
        this.persistence = new Persistence(dstFilename);
        this.index = new HashIndex();
    }

    public void compute() {
        TimeTester.getInstance().setT1(System.currentTimeMillis());
        long t1 = System.currentTimeMillis();
        try {
            reduceDataFile(srcFilename);
        } catch (IOException e) {
            e.printStackTrace();
        }

        long t2 = System.currentTimeMillis();
        String p = "Server InnerFileReducer: " + (t2 - t1) + "ms";
//        System.out.println(p);
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

        Logger logger = LoggerFactory.getLogger(Server.class);
        logger.info(filename + " reduced...");
    }

    private void clearBinlogReducer() {
        for (Binlog binlog : binlogReducer.getBinlogHashMap().values()) {
            long indexOffset;
            long primaryOldValue = binlog.getPrimaryOldValue();
            long primaryValue = binlog.getPrimaryValue();
            if ((indexOffset = index.getOffset(primaryValue)) >= 0) {
                // update other value
                String oldBinlogLine = new String(persistence.read(indexOffset));
                Binlog oldBinlog = BinlogFactory.parse(oldBinlogLine);
                Binlog newBinlog = BinlogReducer.updateOldBinlog(oldBinlog, binlog);
                if (newBinlog != null) {
                    if (primaryValue != newBinlog.getPrimaryValue()) {
                        index.delete(primaryValue);
                    }
                    long offset = persistence.write(newBinlog.toBytes());
                    index.insert(newBinlog.getPrimaryValue(), offset);
                } else {
                    index.delete(primaryValue);
                }
            } else if ((indexOffset = index.getOffset(primaryOldValue)) >= 0) {
                // update key value
                String oldBinlogLine = new String(persistence.read(indexOffset));
                Binlog oldBinlog = BinlogFactory.parse(oldBinlogLine);
                Binlog newBinlog = BinlogReducer.updateOldBinlog(oldBinlog, binlog);
                if (newBinlog != null) {
                    long offset = persistence.write(newBinlog.toBytes());
                    index.delete(primaryOldValue);
                    index.insert(primaryValue, offset);
                } else {
                    index.delete(primaryOldValue);
                }
            } else {
                long offset = persistence.write(binlog.toBytes());
                index.insert(primaryValue, offset);
            }

        }
        binlogReducer.clearBinlogHashMap();
    }

    public Persistence getPersistence() {
        return persistence;
    }

    public Index getIndex() {
        return index;
    }
}
