package com.zbz.zcs;

import com.alibaba.middleware.race.sync.Constants;
import com.sun.xml.internal.bind.v2.runtime.reflect.opt.Const;
import com.zbz.Index;

/**
 * Created by zwy on 17-6-13.
 */
public class FileIndex {
    private String fileName;
    private Index index;
    private String idx;

    public FileIndex(String fileName) {
        this.fileName = fileName;
        this.idx = fileName.substring(Constants.MIDDLE_HOME.length());
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFileName() {
        return this.fileName;
    }

    public void mergeIdx(String idx, String newFileName) {
        this.fileName = newFileName;
        this.idx += ("|" + idx);
    }

    public String getIdx() {
        return idx;
    }
}
