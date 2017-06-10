package com.zbz;

/**
 * Created by Victor on 2017/6/10.
 */
public abstract class Index {
    abstract void insert();

    abstract void delete();

    abstract long getOffset(Binlog binlog);
}
