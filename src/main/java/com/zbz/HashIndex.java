package com.zbz;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Victor on 2017/6/10.
 */
public class HashIndex extends Index {
    private Map<String, Long> indexHashMap = new HashMap<>();

    public void insert(String key, long offset) {
        indexHashMap.put(key, offset);
    }

    public void delete(String key) {
        indexHashMap.remove(key);
    }

    public long getOffset(String key) {
        Long offset = indexHashMap.get(key);
        return offset == null ? Long.MIN_VALUE : offset;
    }

    public Map<String, Long> getIndexHashMap() {
        return indexHashMap;
    }

    public void release() {
        indexHashMap.clear();
        indexHashMap = null;
    }
}
