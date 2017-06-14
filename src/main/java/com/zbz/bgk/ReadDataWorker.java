package com.zbz.bgk;

import com.alibaba.middleware.race.sync.Server;
import com.zbz.*;
import com.zbz.zcs.FileIndex;
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
    private String dstFilename;
    private Index index;
    private Persistence persistence;

    public ReadDataWorker(String schema, String table, String srcFilename, String dstFilename) {
        this.binlogReducer = new BinlogReducer(schema, table);
        this.srcFilename = srcFilename;
        this.dstFilename = dstFilename;
        this.index = new HashIndex();
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

        FileIndex fidx = new FileIndex(index, persistence);
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
            long indexOffset;
            if ((indexOffset = index.getOffset(binlog.getPrimaryValue())) >= 0) {
                // update other value
                String oldBinlogLine = new String(persistence.read(indexOffset));
                Binlog oldBinlog = BinlogFactory.parse(oldBinlogLine);
                Binlog newBinlog = BinlogReducer.updateOldBinlog(oldBinlog, binlog);
                if (newBinlog != null) {
//                    System.out.println("old Binlog:" + oldBinlog);
//                    System.out.println("current1 Binlog:" + binlog);
//                    System.out.println("new Binlog:" + newBinlog);
                    long offset = persistence.write(newBinlog.toBytes());
                    index.insert(newBinlog.getPrimaryValue(), offset);
                } else {
                    index.delete(oldBinlog.getPrimaryValue());
                }
            } else if ((indexOffset = index.getOffset(binlog.getPrimaryOldValue())) >= 0) {
                // update key value
                String oldBinlogLine = new String(persistence.read(indexOffset));
                Binlog oldBinlog = BinlogFactory.parse(oldBinlogLine);
                Binlog newBinlog = BinlogReducer.updateOldBinlog(oldBinlog, binlog);
                if (newBinlog != null) {
//                    System.out.println("old Binlog:" + oldBinlog);
//                    System.out.println("current2 Binlog:" + binlog);
//                    System.out.println("new Binlog:" + newBinlog);
                    long offset = persistence.write(newBinlog.toBytes());
                    index.delete(oldBinlog.getPrimaryValue());
                    index.insert(newBinlog.getPrimaryValue(), offset);
                } else {
                    index.delete(oldBinlog.getPrimaryValue());
                }
            } else {
                long offset = persistence.write(binlog.toBytes());
//                System.out.println("binlog:" + binlog);
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
