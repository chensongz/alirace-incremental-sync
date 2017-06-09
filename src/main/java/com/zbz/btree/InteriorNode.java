package com.zbz.btree;

/**
 * Created by zwy on 17-6-9.
 */
public class InteriorNode extends Node {
    private long[] keys;
    private Node[] pointers;

    public InteriorNode() {
        keys = new long[BPlusTree.N];
        pointers = new Node[BPlusTree.N + 1];
    }

    public int keyIdx(long key) {
        for(int i = 0; i < BPlusTree.N; i++) {
            if(keys[i] ==  key) {
                return i;
            }
        }
        return -1;
    }
}
