package com.zbz.btree;

/**
 * Created by zwy on 17-6-9.
 */
public class BPlusTree {
    public static int N = 4;

    private Node root;

    public BPlusTree(long initPk) {
        root = new InteriorNode();
    }
}
