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
        return indexHashMap.get(key);
    }
}
