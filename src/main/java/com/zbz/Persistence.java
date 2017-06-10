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
    public static int NUM = 8;
    public static int CHAR = 10;

    private FileChannel fc;
    private long recordOffset;
    private int RECORD_WIDTH;

    public Persistence(String filename) {
        try {
            fc = new RandomAccessFile(filename, "rw").getChannel();
            recordOffset = 0;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void init(Table table) {
        //TODO compute record width
        RECORD_WIDTH = 0;
    }

    public long insert(Record record) {
        long ret = 0;
        try {
            ByteBuffer recordBytes = record.toBytes();
            fc.write(recordBytes, recordOffset);
            ret = recordOffset;
            recordOffset += RECORD_WIDTH;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ret;
    }

    public Record query(long offset) {
        ByteBuffer recordBytes = ByteBuffer.allocate(RECORD_WIDTH);
        try {
            fc.read(recordBytes, offset);
            return Record.parse(recordBytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void close() {
        try {
            fc.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
