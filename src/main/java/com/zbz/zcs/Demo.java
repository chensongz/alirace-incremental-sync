package com.zbz.zcs;

import com.alibaba.middleware.race.sync.Constants;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

import com.zbz.*;

/**
 * Created by zhuchensong on 6/9/17.
 */
public class Demo {

    private String schema = "middleware3";
    private String table = "student";

    public static void main(String[] args) {
        Demo demo = new Demo();
        demo.run();
    }

    public void run() {
        long t1 = System.currentTimeMillis();
        List<FileIndex> fileIndices = inFileReduce();
        long t2 = System.currentTimeMillis();
        System.out.println("Demo multi-thread stage1: " + (t2 - t1) + " ms");

        long wtime = 0;
        long rtime = 0;
        for(FileIndex index: fileIndices) {
            wtime += index.getPersist().getWtime();
            rtime += index.getPersist().getRtime();
        }

        System.out.println("Read Time: " + rtime / Constants.DATA_FILE_NUM);
        System.out.println("Write Time: " + wtime / Constants.DATA_FILE_NUM);


//        t1 = System.currentTimeMillis();
//        List<FileIndex> result = commonReduce(Constants.DATA_FILE_NUM, fileIndices);
//        t2 = System.currentTimeMillis();
//        System.out.println("Demo multi-thread stage2: " + (t2 - t1) + " ms");


//        FileIndex f = result.get(0);
//        Index idx = f.getIndex();
//        Persistence per = f.getPersist();
//        for(long i = 600; i < 700; i++) {
//            long offset = idx.getOffset(i);
//            if (offset >= 0) {
//                String binlogLine = new String(per.read(offset));
//                System.out.println(binlogLine);
//            }
//        }
    }

    private List<FileIndex> commonReduce(int n, List<FileIndex> fileIndices) {

        int nn = n;
        try {
            List<FileIndex> reducedIndices = fileIndices;
            while(nn > 2) {
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

        ForkJoinPool forkJoinPool = new ForkJoinPool(); //todo
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
