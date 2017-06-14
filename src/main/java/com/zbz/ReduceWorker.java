package com.zbz;

import com.alibaba.middleware.race.sync.Constants;
import com.zbz.zcs.InterFileWorker;
import com.zbz.zcs.FileIndex;
import com.zbz.zcs.InnerFileWorker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

/**
 * Created by zwy on 17-6-14.
 */
public class ReduceWorker implements Runnable {

    private static int END_CNT = 2;

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
        long t1 = System.currentTimeMillis();
        List<FileIndex> fileIndices = inFileReduce();
        long t2 = System.currentTimeMillis();
        System.out.println("Reduce multi-thread stage1: " + (t2 - t1) + " ms");

        t1 = System.currentTimeMillis();
        List<FileIndex> result = interFileReduce(Constants.DATA_FILE_NUM, fileIndices);
        t2 = System.currentTimeMillis();
        System.out.println("Reduce multi-thread stage2: " + (t2 - t1) + " ms");

        //todo 调用卞老师
        FileIndex f = result.get(0);
        Index idx = f.getIndex();
        Persistence per = f.getPersist();
        for(long i = 600; i < 700; i++) {
            long offset = idx.getOffset(i);
            if (offset >= 0) {
                String binlogLine = new String(per.read(offset));
                System.out.println(binlogLine);
            }
        }
    }

    private List<FileIndex> interFileReduce(int n, List<FileIndex> fileIndices) {
        int nn = n;
        try {
            List<FileIndex> reducedIndices = fileIndices;
            while(nn > END_CNT) {
                ForkJoinPool forkJoinPool = new ForkJoinPool();
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
        for(int i = 0; i < Constants.DATA_FILE_NUM; i++) {
            dataFiles.add(Constants.getDataFile(i));
        }
        ForkJoinPool forkJoinPool = new ForkJoinPool();
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
