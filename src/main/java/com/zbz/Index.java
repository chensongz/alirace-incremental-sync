package com.zbz;

/**
 * Created by Victor on 2017/6/10.
 */
abstract public class Index {
    abstract void insert(long key, long offset);

    abstract void delete(long key);

    abstract long getOffset(long key);
}
