package com.zbz.zcs;

import com.zbz.Index;
import com.zbz.zwy.Persistence;

/**
 * Created by zwy on 17-6-13.
 */
public class FileIndex {
    private Persistence persist;
    private String fileName;
    private Index index;

    public FileIndex(String fileName, Index index, Persistence persist) {
        this.fileName = fileName;
        this.persist = persist;
        this.index = index;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFileName() {
        return this.fileName;
    }
}
