package com.zbz.zcs;

import com.alibaba.middleware.race.sync.Server;
import com.zbz.Index;
import com.zbz.InterFileReducer;
import com.zbz.Persistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.RecursiveTask;

/**
 * Created by zwy on 17-6-13.
 */
public class InterFileWorker extends RecursiveTask<List<FileIndex>> {
    private List<FileIndex> fileList;

    public InterFileWorker(List<FileIndex> fileList) {
        this.fileList = fileList;
    }

    @Override
    protected List<FileIndex> compute() {
        Logger logger = LoggerFactory.getLogger(Server.class);

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

            logger.info(basePersistence.getFilename() + " <- " + appendPersistence.getFilename()
                    + " inter reduce start");
            long t1 = System.currentTimeMillis();
            worker.compute();
            long t2 = System.currentTimeMillis();
            logger.info(basePersistence.getFilename() + " <- " + appendPersistence.getFilename()
                    + " inter reduce: " + (t2 - t1) + " ms");

            index1.release();
            ret.add(index0);
        } else if (len == 1) {
            FileIndex index0 = fileList.get(0);
            ret.add(index0);
        } else {
            InterFileWorker reducer1, reducer2;
            if ((len & 0x1) > 0) {
                List<FileIndex> fileList1 = new ArrayList<>(len - 1);
                List<FileIndex> fileList2 = new ArrayList<>(1);

                for (int i = 0; i < len; i++) {
                    if (i < len - 1) {
                        fileList1.add(fileList.get(i));
                    } else {
                        fileList2.add(fileList.get(i));
                    }
                }

                reducer1 = new InterFileWorker(fileList1);
                reducer2 = new InterFileWorker(fileList2);

                reducer1.fork();
                reducer2.fork();

                ret.addAll(reducer1.join());
                ret.addAll(reducer2.join());
            } else {
                List<InterFileWorker> reducers = new ArrayList<>();
                InterFileWorker reducer0;
                for (int i = 0; i < len; i += 2) {
                    List<FileIndex> fileList1 = new ArrayList<>(2);
                    fileList1.add(fileList.get(i));
                    fileList1.add(fileList.get(i + 1));

                    reducer0 = new InterFileWorker(fileList1);
                    reducers.add(reducer0);
                }
                for (InterFileWorker reducer : reducers) {
                    reducer.fork();
                }
                for (InterFileWorker reducer : reducers) {
                    ret.addAll(reducer.join());
                }
            }
        }
        fileList.clear();
        return ret;
    }
}
