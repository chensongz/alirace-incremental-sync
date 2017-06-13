package com.zbz.zcs;

import com.alibaba.middleware.race.sync.Constants;

import java.io.File;
import java.util.List;
import java.util.concurrent.RecursiveAction;

/**
 * Created by zwy on 17-6-13.
 */
public class CommonReducer extends RecursiveAction {
    private int round;
    private List<String> fileList;

    public CommonReducer(List<String> fileList, int round) {
        this.round = round;
        this.fileList = fileList;
    }
    @Override
    protected void compute() {
        int len = fileList.size();
        if (len == 2) {
            String file1 = fileList.get(0);
            String file2 = fileList.get(1);
            String newFile = getNewFileName(file1);
            try {
                File f = new File(newFile);
                if(!f.exists()) {
                    f.createNewFile();
//                    new File(file1).delete();
//                    new File(file2).delete();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (len == 1) {
            String oldFile = fileList.get(0);
            String newFile = getNewFileName(oldFile);
            try {
                File f = new File(oldFile);
                File nf = new File(newFile);
                if (f.exists()) { f.renameTo(nf); }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            CommonReducer reducer1, reducer2;
            if ((len & 0x1) > 0) {
                reducer1 = new CommonReducer(fileList.subList(0, len - 1), round);
                reducer2 = new CommonReducer(fileList.subList(len - 1, len), round);
            } else {
                reducer1 = new CommonReducer(fileList.subList(0, len / 2), round);
                reducer2 = new CommonReducer(fileList.subList(len / 2, len), round);
            }
            reducer1.fork();
            reducer2.fork();
            reducer1.join();
            reducer2.join();
        }
    }

    private String getNewFileName(String oldName1) {
        String dir = Constants.MIDDLE_HOME;
        return dir + "/" + (round + 1) + Integer.parseInt(oldName1.substring(oldName1.length() - 1)) / 2;
    }
}
