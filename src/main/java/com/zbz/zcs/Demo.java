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
        inFileReduce();
//        reduceTenToFive();
//        reduceFiveToThree();
//        reduceThreeToOne();
        commonReduce(1, 10);
    }

    public void commonReduce(int round, int n) {
        if ( round > 3 ) return;
        List<String> files = new ArrayList<>(n);
        for(int i = 0; i < n; i++) {
            files.add(Constants.MIDDLE_HOME + "/" + round + i);
            System.out.println(files.get(i));
        }

        ForkJoinPool forkJoinPool = new ForkJoinPool();
        CommonReducer reducer = new CommonReducer(files, round);
        Future result = forkJoinPool.submit(reducer);

        try {
            result.get();
        } catch (Exception e) {
            e.printStackTrace();
        }

        commonReduce(round + 1, (n & 0x1) > 0 ? (n / 2 + 1) : n / 2);
    }

    private void inFileReduce() {
        List<String> dataFiles = new ArrayList<>(Constants.DATA_FILE_NUM);
        for(int i = 0; i < Constants.DATA_FILE_NUM; i++) {
            dataFiles.add(Constants.getDataFile(i));
        }

        ForkJoinPool forkJoinPool = new ForkJoinPool(); //todo
        InFileReduce binlogReducerTask = new InFileReduce(dataFiles);
        Future result = forkJoinPool.submit(binlogReducerTask);
        try {
            result.get();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void reduceTenToFive() {
        //10-19 -> 20-24
        List<String> files = new ArrayList<>(10);
        for(int i = 0; i < 10; i++) {
            files.add(Constants.MIDDLE_HOME + "/1" + i);
        }
        ForkJoinPool forkJoinPool = new ForkJoinPool(); //todo
        ReduceTenToFive binlogReducerTask = new ReduceTenToFive(files);
        Future result = forkJoinPool.submit(binlogReducerTask);
        try {
            result.get();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void reduceFiveToThree() {
        //20-24 -> 30-31,24
        List<String> files = new ArrayList<>(4);
        for(int i = 0; i < 4; i++) {
            files.add(Constants.MIDDLE_HOME + "/2" + i);
        }
        ForkJoinPool forkJoinPool = new ForkJoinPool(); //todo
        ReduceFiveToThree binlogReducerTask = new ReduceFiveToThree(files);
        Future result = forkJoinPool.submit(binlogReducerTask);
        try {
            result.get();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void reduceThreeToOne() {
        //31-32,24 -> 41
    }
}
