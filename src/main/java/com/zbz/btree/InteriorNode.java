package com.zbz.btree;

import java.util.LinkedList;

/**
 * Created by zwy on 17-6-9.
 */
public class InteriorNode extends Node {

    public InteriorNode() {
        super(new InteriorElement());
    }

    public InteriorElement insert(long key) {
        InteriorElement newElem = new InteriorElement();
        newElem.key = key;
        super.insert(newElem);
        return newElem;
    }

    public InteriorElement getElement(long key) {
        InteriorElement curr = (InteriorElement) tail;
        while (curr != head) {
            if (key >= curr.key) {
                break;
            }
        }
        return curr;
    }
}
