package com.zbz.zcs;

import com.alibaba.middleware.race.sync.Constants;

import java.io.File;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.RecursiveTask;

/**
 * Created by zwy on 17-6-13.
 */
public class CommonReducer extends RecursiveTask<List<FileIndex>> {
    private int pre;
    private int round;
    private List<FileIndex> fileList;

    public CommonReducer(List<FileIndex> fileList, int round, int pre) {
        this.pre = pre;
        this.round = round;
        this.fileList = fileList;
    }
    @Override
    protected List<FileIndex> compute() {
        int len = fileList.size();
        List<FileIndex> ret = new ArrayList<>();
        if (len == 2) {
            FileIndex index0 = fileList.get(0);
            FileIndex index1 = fileList.get(1);

            String newFile = getNewFileName(index0.getFileName());
            persist(newFile);

            index0.mergeIdx(index1.getIdx(), newFile);

            persist(newFile);

            ret.add(index0);
        } else if (len == 1) {
            FileIndex index0 = fileList.get(0);
            String oldFile = index0.getFileName();
            String newFile = getNewFileName(oldFile);

            index0.setFileName(newFile);

            persist(newFile, oldFile);
            ret.add(index0);
        } else {
            CommonReducer reducer1, reducer2;
            if ((len & 0x1) > 0) {
                reducer1 = new CommonReducer(fileList.subList(0, len - 1), round, pre);
                reducer2 = new CommonReducer(fileList.subList(len - 1, len), round, pre + len - 1);

                reducer1.fork();
                reducer2.fork();

                ret.addAll(reducer1.join());
                ret.addAll(reducer2.join());
            } else {
                List<CommonReducer> reducers = new ArrayList<>();
                CommonReducer reducer0;
                for(int i = 0; i < len; i += 2) {
                    reducer0 = new CommonReducer(fileList.subList(i, i + 2), round, pre + i);
                    reducers.add(reducer0);
                }
                for(CommonReducer reducer: reducers) {
                    reducer.fork();
                }
                for(CommonReducer reducer: reducers) {
                    ret.addAll(reducer.join());
                }
            }
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

    private void persist(String newFile, String oldFile) {
        try {
            File f = new File(oldFile);
            File nf = new File(newFile);
            if (f.exists()) { f.renameTo(nf); }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String getNewFileName(String oldName1) {
        String dir = Constants.MIDDLE_HOME;
        return dir + "/" + (round + 1) + ((pre & 0x1) > 0 ? (pre / 2 + 1) : (pre / 2));
    }
}
