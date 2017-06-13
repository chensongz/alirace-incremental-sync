package com.zbz;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Victor on 2017/6/10.
 */
public class HashIndex extends Index {
    private Map<Long, Long> indexHashMap = new HashMap<>();

    public void insert(long key, long offset) {
        indexHashMap.put(key, offset);
    }

    public void delete(long key) {
        indexHashMap.remove(key);
    }

    public long getOffset(long key) {
        Long offset = indexHashMap.get(key);
        return offset == null ? Long.MIN_VALUE : offset;
    }

    public Map<Long, Long> getIndexHashMap() {
        return indexHashMap;
    }
}
