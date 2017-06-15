package com.zbz;

import java.util.Map;

/**
 * Created by Victor on 2017/6/10.
 */
abstract public class Index {
    abstract public void insert(String key, long offset);

    abstract public void delete(String key);

    abstract public long getOffset(String key);

    abstract public Map<String, Long> getIndexHashMap();

    abstract public void release();
}
