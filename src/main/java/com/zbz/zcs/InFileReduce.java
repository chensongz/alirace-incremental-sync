package com.zbz.zcs;

import com.alibaba.middleware.race.sync.Constants;
import com.zbz.bgk.ReadDataWorker;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RecursiveTask;

/**
 * Created by zwy on 17-6-13.
 */
public class InFileReduce extends RecursiveTask<List<FileIndex>> {

    private List<String> fileList;
    private String schema;
    private String table;

    public InFileReduce(List<String> fileList, String schema, String table) {
        this.fileList = fileList;
        this.schema = schema;
        this.table = table;
    }

    @Override
    protected List<FileIndex> compute() {
        int len = fileList.size();
        List<FileIndex> ret = new ArrayList<>();
        if (len == 1) {

            String dataFileName = fileList.get(0);
            String reducedFileName = getNewFileName(dataFileName);

            ReadDataWorker worker = new ReadDataWorker(schema, table, dataFileName, reducedFileName);
            FileIndex fileIndex = worker.compute();

            ret.add(fileIndex);
        } else {
            //dispatch tasks
            InFileReduce combineTask1
                    = new InFileReduce(fileList.subList(0, len / 2), schema, table);
            InFileReduce combineTask2
                    = new InFileReduce(fileList.subList(len / 2, len), schema, table);
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
