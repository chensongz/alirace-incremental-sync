package com.zbz.zcs;

import com.alibaba.middleware.race.sync.Constants;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

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


        t1 = System.currentTimeMillis();
        List<FileIndex> result = commonReduce(1, 10, fileIndices);
        t2 = System.currentTimeMillis();
        System.out.println("Demo multi-thread stage2: " + (t2 - t1) + " ms");
    }

    private List<FileIndex> commonReduce(int round, int n, List<FileIndex> fileIndices) {
        if(n <= 1) return fileIndices;

        ForkJoinPool forkJoinPool = new ForkJoinPool();
        CommonReducer reducer = new CommonReducer(fileIndices, round, 0);
        Future<List<FileIndex>> result = forkJoinPool.submit(reducer);

        List<FileIndex> reducedIndices = null;

        try {
            reducedIndices = result.get();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("==================================================");

        return commonReduce(round + 1, (n >>> 1) + ((n & 0x1) > 0 ? 1 : 0), reducedIndices);
    }

    private List<FileIndex> inFileReduce() {
        List<String> dataFiles = new ArrayList<>(Constants.DATA_FILE_NUM);
        for(int i = 0; i < Constants.DATA_FILE_NUM; i++) {
            dataFiles.add(Constants.getDataFile(i));
        }

        ForkJoinPool forkJoinPool = new ForkJoinPool(); //todo
        InFileReduce binlogReducerTask = new InFileReduce(dataFiles, schema, table);
        Future<List<FileIndex>> result = forkJoinPool.submit(binlogReducerTask);
        try {
            return result.get();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
