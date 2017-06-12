package com.zbz.zcs;

import com.alibaba.middleware.race.sync.Constants;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RecursiveAction;

/**
 * Created by zwy on 17-6-13.
 */
public class InFileReduce extends RecursiveAction {

    private List<String> fileList;

    public InFileReduce(List<String> fileList) {
        this.fileList = fileList;
    }

    @Override
    protected void compute() {
        int len = fileList.size();
        if (len == 1) {
            String newFile = getNewFileName(fileList.get(0));
            try {
                File f = new File(newFile);
                if(!f.exists()) {
                    f.createNewFile();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            InFileReduce combineTask1 = new InFileReduce(fileList.subList(0, len / 2));
            InFileReduce combineTask2 = new InFileReduce(fileList.subList(len / 2, len));
            combineTask1.fork();
            combineTask2.fork();
            combineTask1.join();
            combineTask2.join();
        }
    }

    private String getNewFileName(String oldName) {
        String dir = Constants.MIDDLE_HOME;
        return dir + "/1" + oldName.substring(oldName.length() - 1);
    }
}
