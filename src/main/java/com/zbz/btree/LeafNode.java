package com.zbz.btree;

/**
 * Created by zwy on 17-6-9.
 */
public class LeafNode extends Node {

    protected LeafNode next;

    public LeafNode() {
        super(new LeafElement());
    }

    public void insert(long key, Object data) {
        LeafElement newElem = new LeafElement();
        newElem.key = key;
        newElem.delete = false;
        newElem.offset = fileInsert(key, data);
        super.insert(newElem);
    }

    private long fileInsert(long key, Object data) {
        return 0;
    }
}
