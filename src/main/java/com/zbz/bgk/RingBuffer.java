package com.zbz.bgk;

import com.zbz.DataConstants;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by bgk on 6/23/17.
 */
public class RingBuffer<T> {
    private static final int capacity = DataConstants.RINGBUFFER_CAPACITY;
    private static final int remain = DataConstants.RINGBUFFER_CAPACITY - 1;

    private T[] buf;
    private volatile int readPosition = 0;
    private volatile int writePosition = 0;

    public RingBuffer(T[] buf) {
        this.buf = buf;
    }

    public T put(T value) {
        if (writePosition - readPosition >= capacity) {
            return null;
        }
        buf[writePosition & remain] = value;
        writePosition++;
        return value;
    }


    public T get() {
        if((writePosition - readPosition) <= 0) {
            return null;
        }
        T value = buf[readPosition & remain];
        readPosition++;
        return value;
    }

}
