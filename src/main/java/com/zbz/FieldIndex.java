package com.zbz;

import gnu.trove.map.hash.TByteByteHashMap;

/**
 * Created by bgk on 6/18/17.
 */
public class FieldIndex {
    private boolean isInit = false;

    private TByteByteHashMap tByteByteHashMap = new TByteByteHashMap();
    private byte index = 0;

    public void put(byte field) {
        tByteByteHashMap.put(field, index++);
    }

    public byte get(byte field) {
        return tByteByteHashMap.get(field);
    }

    public void setInit(boolean isInit) {
        this.isInit = isInit;
    }

    public boolean isInit() {
        return isInit;
    }
}
