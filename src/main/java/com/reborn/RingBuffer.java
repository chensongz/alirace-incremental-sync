package com.reborn;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

/**
 * Created by zhuchensong on 6/27/17.
 */
public class RingBuffer {
    private static final int capacity = DataConstants.MAPPED_BUFFER_CAPACITY;
    private static final int andIndex = DataConstants.MAPPED_BUFFER_CAPACITY - 1;

    private volatile int writeSeq = 0;
    private volatile int readSeq = 0;

    private ByteBuffer[] buffer;

    public RingBuffer() {
        this.buffer = new ByteBuffer[capacity];
    }

    public boolean put(ByteBuffer mappedByteBuffer) {
        if (writeSeq - readSeq >= capacity) {
            return false;
        }
        buffer[writeSeq & andIndex] = mappedByteBuffer;
        writeSeq++;
        return true;
    }

    public ByteBuffer get() {
        if (writeSeq - readSeq <= 0) {
            return null;
        }
        ByteBuffer mappedByteBuffer = buffer[readSeq & andIndex];
        readSeq++;
        return mappedByteBuffer;
    }

}