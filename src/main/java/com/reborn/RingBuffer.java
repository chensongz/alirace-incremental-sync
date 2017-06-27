package com.reborn;

/**
 * Created by zhuchensong on 6/27/17.
 */
public class RingBuffer {
    private static final int capacity = DataConstants.RINGBUFFER_SIZE;
    private static final int andIndex = DataConstants.RINGBUFFER_SIZE - 1;

    private volatile long writeSeq = 0;
    private volatile long readSeq = 0;

    private byte[] buffer;

    public RingBuffer() {
        this.buffer = new byte[capacity];
    }

    public boolean put(byte[] bytes, int len) {
        if (writeSeq - readSeq >= capacity - len) {
            return false;
        }
        int startIndex = (int) (writeSeq & andIndex);
        int endIndex = (int) ((writeSeq + len) & andIndex);
        if (endIndex > startIndex) {
            System.arraycopy(bytes, 0, buffer, startIndex, len);
        } else {
            int firstLen = capacity - startIndex;
            System.arraycopy(bytes, 0, buffer, startIndex, firstLen);
            System.arraycopy(bytes, firstLen, buffer, 0, len - firstLen);
        }
        writeSeq += len;
        return true;
    }

    public byte[] get(byte[] bytes, int len) {
        if (writeSeq - readSeq < len) {
            return null;
        }
        int startIndex = (int) (readSeq & andIndex);
        int endIndex = (int) ((readSeq + len) & andIndex);
        if (endIndex > startIndex) {
            System.arraycopy(buffer, startIndex, bytes, 0, len);
        } else {
            int firstLen = capacity - startIndex;
            System.arraycopy(buffer, startIndex, bytes, 0, firstLen);
            System.arraycopy(buffer, 0, bytes, firstLen, len - firstLen);
        }
        readSeq += len;
        return bytes;
    }

}