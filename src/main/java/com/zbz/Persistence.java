package com.zbz;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by zwy on 17-6-10.
 */
public class Persistence {

    public static final int INT_SIZE = Integer.SIZE / Byte.SIZE;

    private String filename;
    private FileChannel fc;
    private int FIXED_WIDTH;
    private long currentOffset;

    public Persistence(String filename) {
        this(filename, -1);
    }

    public Persistence(String filename, int width) {
        this.filename = filename;
        try {
            fc = new RandomAccessFile(filename, "rw").getChannel();
            currentOffset = 0;
            FIXED_WIDTH = width;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            fc.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public long write(byte[] bytes, long offset) {
        if (FIXED_WIDTH < 0) return -1;

        ByteBuffer buf = ByteBuffer.allocate(FIXED_WIDTH);
        buf.put(bytes);
        try {
            fc.write(buf, offset);
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
        return offset;
    }

    public long write(byte[] bytes) {
        long ret = currentOffset;
        if (FIXED_WIDTH < 0) {
            //not fixed width
            int messageLength = bytes.length;
            int totalLength = messageLength + INT_SIZE;

            ByteBuffer buf = ByteBuffer.allocate(totalLength);
            buf.putInt(messageLength);
            buf.put(bytes);
            buf.flip();
            try {
                fc.write(buf, currentOffset);
                currentOffset += totalLength;
            } catch (IOException e) {
                e.printStackTrace();
                ret = -1;
            }
        } else {
            //fixed length
            write(bytes, currentOffset);
            currentOffset += FIXED_WIDTH;
        }
        return ret;
    }

    public byte[] read(long offset) {
        ByteBuffer mb;
        long readOffset = offset;
        try {
            if (FIXED_WIDTH < 0) {
                //not fixed width
                ByteBuffer widthBuf = ByteBuffer.allocate(INT_SIZE);
                fc.read(widthBuf, readOffset);
                widthBuf.flip();
                int width = widthBuf.getInt();
                mb = ByteBuffer.allocate(width);
                readOffset += INT_SIZE;
            } else {
                //fixed width
                mb = ByteBuffer.allocate(FIXED_WIDTH);
            }
            fc.read(mb, readOffset);
            return mb.array();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public String getFilename() {
        return filename;
    }
}
