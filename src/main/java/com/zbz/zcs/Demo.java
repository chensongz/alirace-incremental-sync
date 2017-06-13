package com.zbz.zcs;

import com.alibaba.middleware.race.sync.Constants;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

/**
 * Created by zhuchensong on 6/9/17.
 */
public class Demo {

    public static void main(String[] args) {
        Demo demo = new Demo();
        demo.run();
    }

    public void run() {
        List<FileIndex> fileIndices = inFileReduce();
        List<FileIndex> result = commonReduce(1, 10, fileIndices);
        for(FileIndex idx: result) {
            System.out.println(idx.getIdx());
        }
    }

    private List<FileIndex> commonReduce(int round, int n, List<FileIndex> fileIndices) {
        System.out.println("common " + n);
        if(n <= 2) return fileIndices;

        ForkJoinPool forkJoinPool = new ForkJoinPool();
        CommonReducer reducer = new CommonReducer(fileIndices, round, 0);
        Future<List<FileIndex>> result = forkJoinPool.submit(reducer);

        List<FileIndex> reducedIndices = null;

        try {
            reducedIndices = result.get();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return commonReduce(round + 1, (n >>> 1) + ((n & 0x1) > 0 ? 1 : 0), reducedIndices);
    }

    private List<FileIndex> inFileReduce() {
        List<String> dataFiles = new ArrayList<>(Constants.DATA_FILE_NUM);
//        for(int i = 0; i < 8; i++) {
//            dataFiles.add(Constants.getDataFile(i));
//        }
        for(int i = 0; i < Constants.DATA_FILE_NUM; i++) {
            dataFiles.add(Constants.getDataFile(i));
        }

        ForkJoinPool forkJoinPool = new ForkJoinPool(); //todo
        InFileReduce binlogReducerTask = new InFileReduce(dataFiles);
        Future<List<FileIndex>> result = forkJoinPool.submit(binlogReducerTask);
        try {
            return result.get();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
