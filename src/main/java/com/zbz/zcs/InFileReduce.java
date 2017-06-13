package com.zbz.zcs;

import com.alibaba.middleware.race.sync.Constants;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RecursiveTask;

/**
 * Created by zwy on 17-6-13.
 */
public class InFileReduce extends RecursiveTask<List<FileIndex>> {

    private List<String> fileList;

    public InFileReduce(List<String> fileList) {
        this.fileList = fileList;
    }

    @Override
    protected List<FileIndex> compute() {
        int len = fileList.size();
        List<FileIndex> ret = new ArrayList<>();
        if (len == 1) {

            String dataFileName = fileList.get(0);
            String reducedFileName = getNewFileName(dataFileName);

            System.out.println("1111 " + reducedFileName);

            FileIndex fileIndex = new FileIndex(reducedFileName);

            persist(reducedFileName);

            ret.add(fileIndex);
        } else {
            InFileReduce combineTask1 = new InFileReduce(fileList.subList(0, len / 2));
            InFileReduce combineTask2 = new InFileReduce(fileList.subList(len / 2, len));
            combineTask1.fork();
            combineTask2.fork();
            ret.addAll(combineTask1.join());
            ret.addAll(combineTask2.join());
        }
        return ret;
    }

    private void persist(String newFile) {
        try {
            File f = new File(newFile);
            if(!f.exists()) {
                f.createNewFile();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String getNewFileName(String oldName) {
        String dir = Constants.MIDDLE_HOME;
        return dir + "/1" + oldName.substring(oldName.length() - 1);
    }
}
