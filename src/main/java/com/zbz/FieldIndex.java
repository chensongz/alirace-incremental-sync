package com.zbz;

import gnu.trove.map.hash.TIntByteHashMap;

/**
 * Created by bgk on 6/18/17.
 */
public class FieldIndex {
    private boolean isInit = false;

    private TIntByteHashMap tByteByteHashMap = new TIntByteHashMap();
    private byte index = 0;

    public void put(int field) {
        tByteByteHashMap.put(field, index++);
    }

    public byte get(int field) {
        return tByteByteHashMap.get(field);
    }

    public byte getIndex() {
        return index;
    }

    public void setInit(boolean isInit) {
        this.isInit = isInit;
    }

    public boolean isInit() {
        return isInit;
    }
}
