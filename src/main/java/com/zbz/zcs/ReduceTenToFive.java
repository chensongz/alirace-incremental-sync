package com.zbz.zcs;

import com.alibaba.middleware.race.sync.Constants;

import java.io.File;
import java.util.List;
import java.util.concurrent.RecursiveAction;

/**
 * Created by zwy on 17-6-13.
 */
public class ReduceTenToFive extends RecursiveAction {
    private List<String> fileList;

    public ReduceTenToFive(List<String> fileList) {
        this.fileList = fileList;
    }
    @Override
    protected void compute() {
        int len = fileList.size();
        if (len == 2) {
            String newFile = getNewFileName(fileList.get(1));
            try {
                File f = new File(newFile);
                System.out.println(f.exists() + ":" + f.getName());
                if(!f.exists()) {
                    f.createNewFile();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        } else {
            ReduceTenToFive combineTask1 = new ReduceTenToFive(fileList.subList(0, 2));
            ReduceTenToFive combineTask2 = new ReduceTenToFive(fileList.subList(2, len));
            combineTask1.fork();
            combineTask2.fork();
            combineTask1.join();
            combineTask2.join();
        }
    }

    private String getNewFileName(String oldName) {
        String dir = Constants.MIDDLE_HOME;
        System.out.println(oldName);
        System.out.println(dir + "/2" + Integer.parseInt(oldName.substring(oldName.length() - 1)) / 2);
        return dir + "/2" + Integer.parseInt(oldName.substring(oldName.length() - 1)) / 2;
    }
}
