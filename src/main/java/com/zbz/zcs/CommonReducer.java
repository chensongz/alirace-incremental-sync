package com.zbz.zcs;

import com.alibaba.middleware.race.sync.Constants;
import com.zbz.Index;
import com.zbz.InterFileReducer;
import com.zbz.Persistence;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.RecursiveTask;

/**
 * Created by zwy on 17-6-13.
 */
public class CommonReducer extends RecursiveTask<List<FileIndex>> {
    private int round;
    private List<FileIndex> fileList;

    public CommonReducer(List<FileIndex> fileList) {
        this.fileList = fileList;
    }

    @Override
    protected List<FileIndex> compute() {
        int len = fileList.size();

        List<FileIndex> ret = new ArrayList<>();
        if (len == 2) {
            FileIndex index0 = fileList.get(0);
            FileIndex index1 = fileList.get(1);

            Persistence basePersistence = index0.getPersist();
            Persistence appendPersistence = index1.getPersist();
            Index baseIndex = index0.getIndex();
            Index appendIndex = index1.getIndex();

            InterFileReducer worker = new InterFileReducer(
                    baseIndex, appendIndex, basePersistence, appendPersistence);
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

                reducer1 = new CommonReducer(fileList1);
                reducer2 = new CommonReducer(fileList2);

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

                    reducer0 = new CommonReducer(fileList1);
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
        fileList.clear();
        return ret;
    }
}
