package com.zbz;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Victor on 2017/6/10.
 */
public class HashIndex extends Index {
    Map<Long, Long> indexHashMap = new HashMap<>();
    public void insert() {

    }

    public void delete() {

    }

    public long getOffset(Binlog binlog) {
        return indexHashMap.get(binlog.getPrimaryValue());
    }
}
