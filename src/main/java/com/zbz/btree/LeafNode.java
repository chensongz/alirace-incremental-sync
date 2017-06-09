package com.zbz.btree;

/**
 * Created by zwy on 17-6-9.
 */
public class LeafNode extends Node {
    private long[] keys;
    private long[] offsets;
    private Node next;

    public LeafNode() {
        keys = new long[BPlusTree.N];
        offsets = new long[BPlusTree.N];
        next = null;
    }
}
