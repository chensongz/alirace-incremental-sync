package com.zbz.btree;


/**
 * Created by zwy on 17-6-9.
 */
public class BPlusTree {

    private InteriorNode root;
    private LeafNode leafHead;

    public BPlusTree(long initPk, Object data) {
        leafHead = new LeafNode();
        root = new InteriorNode();

        leafHead.setParent(root);
        root.setParent(null);

        leafHead.insert(initPk, data);
        root.insert(initPk);
        root.getElement(initPk).child = leafHead;
    }

    public void insert(long key, Object data) {
        LeafNode target = searchNode(key);
        if (target.hasVacancy()) {
            //leaf has vacancy
            target.insert(key, data);
        } else {

            InteriorNode parent = (InteriorNode) target.getParent();

            //split leaf
            LeafNode newTarget = (LeafNode)target.split();
            if (newTarget.getFirstElement().key < key) {
                newTarget.insert(key, data);
            } else {
                target.insert(key, data);
            }

            //insert new leaf into leaf list
            newTarget.next = target.next;
            target.next = newTarget;


            if (parent.hasVacancy()) {
                //leaf is full while parent has vacancy
                InteriorElement t = parent.insert(newTarget.getFirstElement().key);
                t.child = newTarget;
            } else {
                //both are full
                Node pending = newTarget;
                InteriorElement t;
                while(parent != null && !parent.hasVacancy()) {
                    InteriorNode newParent = (InteriorNode) parent.split();
                    if (newParent.getFirstElement().key < key) {
                        t = newParent.insert(key);
                    } else {
                        t = parent.insert(key);
                    }
                    t.child = pending;
                    InteriorNode preParent = parent.parent;
                    pending = newParent;

                    //can stop
                    if(preParent != null && preParent.hasVacancy()) {
                        t = preParent.insert(key);
                        t.child = pending;
                        break;
                    } else if (preParent == null) {
                        //a full root reached
                        //parent == root
                        InteriorNode oldRoot = root;
                        InteriorNode newOldRoot = (InteriorNode) root.split();
                        if (newOldRoot.getFirstElement().key < key) {
                            t = newOldRoot.insert(key);
                        } else {
                            t = oldRoot.insert(key);
                        }
                        t.child = pending;
                        root = new InteriorNode();
                        root.insert(oldRoot.getFirstElement().key).child = oldRoot;
                        root.insert(newOldRoot.getFirstElement().key).child = newOldRoot;
                        break;
                    }

                    parent = preParent;
                }
            }
        }
    }

    public LeafNode searchNode(long key) {
        return doSearchNode(root, key);
    }

    private LeafNode doSearchNode(Node node, long key) {
        if (node instanceof LeafNode) {
            return (LeafNode)node;
        } else {
            InteriorElement curr = (InteriorElement)node.tail;
            while (curr != node.head) {
                if (curr.key <= key) {
                    break;
                }
            }
            return doSearchNode(curr.child, key);
        }
    }
}
