package com.zbz.bgk;

import com.alibaba.middleware.race.sync.Server;
import com.zbz.*;
import com.zbz.zwy.Persistence;
import com.zbz.zwy.TimeTester;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by bgk on 6/13/17.
 */
public class ReadDataWorker {
    private BinlogReducer binlogReducer;
    private String srcFilename;
    private Index index = new HashIndex();
    private Persistence persistence;
    public ReadDataWorker(String schema, String table, String srcFilename, String dstFilename) {
        this.binlogReducer = new BinlogReducer(schema, table);
        this.srcFilename = srcFilename;
        this.persistence = new Persistence(dstFilename);
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
        String p = "Server readDataWorker: " + (t2 - t1) + "ms";
        System.out.println(p);
//        LoggerFactory.getLogger(Server.class).info(p);
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
            long indexOffset = index.getOffset(binlog.getPrimaryValue());
            if (indexOffset < 0) {
                long offset = persistence.write(binlog.toBytes());
                System.out.println("binlog:" + binlog);
                index.insert(binlog.getPrimaryValue(), offset);
            } else {
                String oldBinlogLine = new String(persistence.read(indexOffset));
                Binlog oldBinlog = BinlogFactory.parse(oldBinlogLine);
                Binlog newBinlog = BinlogReducer.updateOldBinlog(oldBinlog, binlog, binlog.getOperation());
                System.out.println("old Binlog:" + oldBinlog);
                System.out.println("new Binlog:" + newBinlog);
                long offset = persistence.write(newBinlog.toBytes());
                index.insert(binlog.getPrimaryValue(), offset);
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
