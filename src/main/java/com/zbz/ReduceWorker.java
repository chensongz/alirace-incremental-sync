package com.zbz;

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.Server;
import com.zbz.zcs.InterFileWorker;
import com.zbz.zcs.FileIndex;
import com.zbz.zcs.InnerFileWorker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zwy on 17-6-14.
 */
public class ReduceWorker implements Runnable {

    private static int END_CNT = 1;

    private String schema;
    private String table;
    private long start;
    private long end;
    private Pool<String> sendPool;

    public ReduceWorker(String schema, String table, long start, long end, Pool<String> sendPool) {
        this.schema = schema;
        this.table = table;
        this.start = start;
        this.end = end;
        this.sendPool = sendPool;
    }

    @Override
    public void run() {

        Logger logger = LoggerFactory.getLogger(Server.class);

        logger.info("inner file reduce start");
        long t1 = System.currentTimeMillis();
        List<FileIndex> fileIndices = inFileReduce();
        long t2 = System.currentTimeMillis();
        logger.info("inner file reduce: " + (t2 - t1) + " ms");

        t1 = System.currentTimeMillis();
        logger.info("inter file reduce start");
        List<FileIndex> result = interFileReduce(Constants.DATA_FILE_NUM, fileIndices);
        t2 = System.currentTimeMillis();
        logger.info("inter file reduce: " + (t2 - t1) + " ms");

        Index baseIndex = result.get(0).getIndex();
        Persistence basePersistence = result.get(0).getPersist();

        t1 = System.currentTimeMillis();
        logger.info("printResult start");
        printResult(baseIndex, basePersistence);
        t2 = System.currentTimeMillis();
        logger.info("printResult: " + (t2 - t1) + " ms");

//        Index appendIndex = result.get(1).getIndex();
//        Persistence appendPersistence = result.get(1).getPersist();

//        t1 = System.currentTimeMillis();
//        FinalReducer finalReducer = new FinalReducer(baseIndex, appendIndex,
//                basePersistence, appendPersistence);
//        finalReducer.compute(start, end, sendPool);
//        t2 = System.currentTimeMillis();
//        logger.info("final reduce: " + (t2 - t1) + " ms");

    }

    private void printResult(Index index, Persistence persistence) {
        for (long i = start + 1; i < end; i++) {
            long offset = index.getOffset(i);
            if (offset >= 0) {
                String binlogLine = new String(persistence.read(offset));
                Binlog binlog = BinlogFactory.parse(binlogLine);
                sendPool.put(binlog.toSendString());
            }
        }
        sendPool.put("NULL");
    }


    private List<FileIndex> interFileReduce(int n, List<FileIndex> fileIndices) {
        int nn = n;
        try {
            List<FileIndex> reducedIndices = fileIndices;
            while (nn > END_CNT) {
                ForkJoinPool forkJoinPool = new ForkJoinPool(10);
                InterFileWorker reducer = new InterFileWorker(reducedIndices);
                Future<List<FileIndex>> result = forkJoinPool.submit(reducer);
                reducedIndices = result.get();
                nn = (nn >>> 1) + ((nn & 0x1) > 0 ? 1 : 0);
            }
            return reducedIndices;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private List<FileIndex> inFileReduce() {
        List<String> dataFiles = new ArrayList<>(Constants.DATA_FILE_NUM);
        for (int i = 0; i < Constants.DATA_FILE_NUM; i++) {
            dataFiles.add(Constants.getDataFile(i));
        }
        ForkJoinPool forkJoinPool = new ForkJoinPool(10);
        InnerFileWorker binlogReducerTask = new InnerFileWorker(dataFiles, schema, table);
        Future<List<FileIndex>> result = forkJoinPool.submit(binlogReducerTask);
        try {
            return result.get();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
