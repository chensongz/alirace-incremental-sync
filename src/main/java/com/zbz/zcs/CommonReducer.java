package com.zbz.zcs;

import com.alibaba.middleware.race.sync.Constants;
import com.zbz.Index;
import com.zbz.bgk.ReadDataWorker2;
import com.zbz.zwy.Persistence;

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
        System.out.println("hahahhahah: " + fileList.toString());
    }

    @Override
    protected List<FileIndex> compute() {
        int len = fileList.size();
        System.out.println("fasdfasdf " + len);
        List<FileIndex> ret = new ArrayList<>();
        if (len == 2) {
            FileIndex index0 = fileList.get(0);
            FileIndex index1 = fileList.get(1);

            Persistence basePersistence = index0.getPersist();
            Persistence appendPersistence = index1.getPersist();
            Index baseIndex = index0.getIndex();
            Index appendIndex = index1.getIndex();

            System.out.println("before worker");
            ReadDataWorker2 worker = new ReadDataWorker2(
                    baseIndex, appendIndex, basePersistence, appendPersistence);
            System.out.println("after worker");
            worker.compute();
            index1.release();

            ret.add(index0);
        } else if (len == 1) {
            FileIndex index0 = fileList.get(0);
            ret.add(index0);
        } else {
            CommonReducer reducer1, reducer2;
            if ((len & 0x1) > 0) {
                List<FileIndex> fileList1 = new ArrayList<>(len - 1);
                List<FileIndex> fileList2 = new ArrayList<>(1);

                for(int i = 0; i < len; i++) {
                    if (i < len - 1) {
                        fileList1.add(fileList.get(i));
                    } else {
                        fileList2.add(fileList.get(i));
                    }
                }

                reducer1 = new CommonReducer(fileList1, round, pre);
                reducer2 = new CommonReducer(fileList2, round, pre + len - 1);

                reducer1.fork();
                reducer2.fork();

                ret.addAll(reducer1.join());
                ret.addAll(reducer2.join());
            } else {
                List<CommonReducer> reducers = new ArrayList<>();
                CommonReducer reducer0;
                for(int i = 0; i < len; i += 2) {
                    List<FileIndex> fileList1 = new ArrayList<>(2);
                    fileList1.add(fileList.get(i));
                    fileList1.add(fileList.get(i + 1));

                    reducer0 = new CommonReducer(fileList1, round, pre + i);
                    reducers.add(reducer0);
                }
                for(CommonReducer reducer: reducers) {
                    reducer.fork();
                }
                for(CommonReducer reducer: reducers) {
                    ret.addAll(reducer.join());
                    System.out.println("oops");
                }
            }
        }
        fileList.clear();
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
