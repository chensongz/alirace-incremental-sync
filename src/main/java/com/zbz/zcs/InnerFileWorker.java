package com.zbz.zcs;

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.Server;
import com.zbz.InnerFileReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RecursiveTask;

/**
 * Created by zwy on 17-6-13.
 */
public class InnerFileWorker extends RecursiveTask<List<FileIndex>> {

    private List<String> fileList;
    private String schema;
    private String table;

    public InnerFileWorker(List<String> fileList, String schema, String table) {
        this.fileList = fileList;
        this.schema = schema;
        this.table = table;
    }

    @Override
    protected List<FileIndex> compute() {
        Logger logger = LoggerFactory.getLogger(Server.class);

        int len = fileList.size();
        List<FileIndex> ret = new ArrayList<>();
        if (len == 1) {
            String dataFileName = fileList.get(0);
            String reducedFileName = getNewFileName(dataFileName);
            InnerFileReducer worker =
                    new InnerFileReducer(schema, table, dataFileName, reducedFileName);

            logger.info("file " + dataFileName + " inner reduce start");
            long t1 = System.currentTimeMillis();
            worker.compute();
            long t2 = System.currentTimeMillis();
            logger.info("file " + dataFileName + " inner reduce: " + (t2 - t1) + " ms");

            FileIndex fileIndex =
                    new FileIndex(worker.getIndex(), worker.getPersistence());
            ret.add(fileIndex);
        } else {
            //dispatch tasks
            InnerFileWorker combineTask1
                    = new InnerFileWorker(fileList.subList(0, len / 2), schema, table);
            InnerFileWorker combineTask2
                    = new InnerFileWorker(fileList.subList(len / 2, len), schema, table);
            combineTask1.fork();
            combineTask2.fork();
            ret.addAll(combineTask1.join());
            ret.addAll(combineTask2.join());
        }
        return ret;
    }

    private String getNewFileName(String oldName) {
        String dir = Constants.MIDDLE_HOME;
        return dir + "/1" + oldName.substring(oldName.length() - 1);
    }
}
