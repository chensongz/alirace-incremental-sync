package com.zbz;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Victor on 2017/6/10.
 */
public class HashIndex extends Index {
    private Map<Long, Long> indexHashMap = new HashMap<>();

    public void insert(Long key, long offset) {
        indexHashMap.put(key, offset);
    }

    public void delete(Long key) {
        indexHashMap.remove(key);
    }

    public long getOffset(Long key) {
        Long offset = indexHashMap.get(key);
        return offset == null ? Long.MIN_VALUE : offset;
    }

    public Map<Long, Long> getIndexHashMap() {
        return indexHashMap;
    }

    public void release() {
        indexHashMap.clear();
        indexHashMap = null;
    }
}
