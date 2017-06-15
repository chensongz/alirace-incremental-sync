package com.zbz;

import java.util.Map;

/**
 * Created by Victor on 2017/6/10.
 */
abstract public class Index {
    abstract public void insert(Long key, long offset);

    abstract public void delete(Long key);

    abstract public long getOffset(Long key);

    abstract public Map<Long, Long> getIndexHashMap();

    abstract public void release();
}
