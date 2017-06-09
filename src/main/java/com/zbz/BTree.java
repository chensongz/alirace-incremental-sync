package com.zbz;

import com.zbz.btree.BPlusTree;

public class BTree {

    private BPlusTree tree;

    public BTree(long initPk) {
        tree = new BPlusTree(initPk);
    }
}
