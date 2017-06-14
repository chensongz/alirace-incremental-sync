package com.zbz.zcs;

import com.zbz.Index;
import com.zbz.Persistence;

/**
 * Created by zwy on 17-6-13.
 */
public class FileIndex {
    private Persistence persist;
    private Index index;

    public FileIndex(Index index, Persistence persist) {
        this.persist = persist;
        this.index = index;
    }

    public Persistence getPersist() {
        return persist;
    }

    public Index getIndex() {
        return index;
    }

    public void release() {
        persist.close();
        persist = null;
        index = null;
    }
}
