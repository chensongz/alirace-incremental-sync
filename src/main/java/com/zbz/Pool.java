package com.zbz;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by zwy on 17-6-12.
 */
public class Pool<T> {
    private static final ConcurrentHashMap<Class, Pool> map = new ConcurrentHashMap<>();

    public synchronized static <E> Pool<E> getPoolInstance(Class<E> type, int capacity) throws IllegalAccessException, InstantiationException {
        if (! map.containsKey(type)) {
            Pool<E> pool = new Pool(capacity);
            map.put(type, pool);
        }
        return map.get(type);
    }

    private BlockingQueue<T> queue;

    public Pool(int capacity) {
        queue = new LinkedBlockingQueue<>(capacity);
    }

    public void put(T elem) {
        try {
            queue.put(elem);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public T poll() {
        try {
            return queue.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }
}
