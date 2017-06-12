package com.zbz.zcs;

import java.util.List;
import java.util.Random;
import java.util.concurrent.RecursiveAction;

/**
 * Created by zhuchensong on 6/12/17.
 */
public class CombineTask extends RecursiveAction {

    private List<String> fileList;
    private Random random = new Random();

    public CombineTask(List<String> fileList) {
        this.fileList = fileList;
    }

    @Override
    protected void compute() {
        int len = fileList.size();
        if (len == 1) {
            // combine binlog file
            try {
//                Thread.sleep(random.nextInt(3) * 1000);
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("combine binlog file: " + fileList.get(0));
        } else {
            CombineTask combineTask1 = new CombineTask(fileList.subList(0, len / 2));
            CombineTask combineTask2 = new CombineTask(fileList.subList(len / 2, len));
            combineTask1.fork();
            combineTask2.fork();
            combineTask1.join();
            combineTask2.join();
        }

    }
}
