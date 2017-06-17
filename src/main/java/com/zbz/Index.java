package com.zbz;

import gnu.trove.map.hash.TLongLongHashMap;

/**
 * Created by Victor on 2017/6/10.
 */
abstract public class Index {
    abstract public void insert(Long key, long offset);

    abstract public void delete(Long key);

    abstract public long getOffset(Long key);

    abstract public TLongLongHashMap getIndexHashMap();

    abstract public void release();
}
