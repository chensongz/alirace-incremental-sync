package com.zbz.btree;

/**
 * Created by zwy on 17-6-9.
 */
public class Element {
    protected Element next;
    protected Element prev;

    protected long key;

    public Element() {
        prev = next = null;
    }
}
