package com.zbz.btree;

/**
 * Created by zwy on 17-6-9.
 */
public class Node {
    protected static final int FILL_FACTOR = 4;

    protected InteriorNode parent;

    protected Element head;
    protected Element tail;
    protected int filled;

    public Node(Element guard) {
        filled = 0;
        guard.key = Long.MIN_VALUE;
        head = tail = guard;
    }

    public void setParent(InteriorNode parent) {
        this.parent = parent;
    }

    public Node getParent() {
        return parent;
    }

    public Element getFirstElement() {
        return head.next;
    }

    public Element getLastElement() {
        return tail;
    }

    public boolean hasVacancy() {
        return filled < FILL_FACTOR;
    }

    public void insert(Element elem) {
        InteriorElement curr = (InteriorElement) tail;
        while (curr != head) {
            if (elem.key >= curr.key) {
                //insert into element list of this node
                if (curr == tail) {
                    tail = elem;
                }
                elem.next = curr.next;
                curr.prev.next = elem;
                elem.prev = curr.prev;
                curr.prev = elem;
            }
        }
        filled++;
    }

    public Node split() {
        if (!hasVacancy()) {
            return null;
        }
        int midIdx = FILL_FACTOR / 2;
        Element curr = head;
        for (int i = -1; i <= midIdx; i++) {
            curr = curr.next;
        }
        tail = curr;
        curr = curr.next;
        tail.next = null;

        Node newNode = doSplit();
        while(curr != null) {
            filled--;
            newNode.insert(curr);
            curr = curr.next;
        }
        return newNode;
    }

    public Node doSplit() {
        if (this instanceof InteriorNode) return new InteriorNode();
        else if (this instanceof LeafNode) return new LeafNode();
        else return null;
    }
}
