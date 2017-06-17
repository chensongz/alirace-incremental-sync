package com.zbz;

import gnu.trove.map.hash.TLongLongHashMap;

/**
 * Created by Victor on 2017/6/10.
 */
public class HashIndex extends Index {
    private TLongLongHashMap indexHashMap = new TLongLongHashMap();

    public void insert(Long key, long offset) {
        indexHashMap.put(key, offset);
    }

    public void delete(Long key) {
        indexHashMap.remove(key);
    }

    public long getOffset(Long key) {
        return indexHashMap.get(key);
    }

    public TLongLongHashMap getIndexHashMap() {
        return indexHashMap;
    }

    public void release() {
        indexHashMap.clear();
        indexHashMap = null;
    }
}
