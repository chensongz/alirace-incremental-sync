package com.zbz.zcs;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

/**
 * Created by zhuchensong on 6/12/17.
 */
public class ForkJoinTester {
    public static void main(String[] args) {
        List<String> fileList = new ArrayList<>();
        int count = 8;
        for (int i = 1; i <= count; i++) {
            fileList.add(i + ".txt");
        }

        ForkJoinPool forkJoinPool = new ForkJoinPool(10);
        CombineTask combineTask = new CombineTask(fileList);
        long startTime = System.currentTimeMillis();
        Future result = forkJoinPool.submit(combineTask);
        try {
            result.get();
        } catch (Exception e) {
            e.printStackTrace();
        }
        long endTime = System.currentTimeMillis();
        System.out.println("All task finished!!\nCost time: " + (endTime - startTime) + " ms");
    }
}
