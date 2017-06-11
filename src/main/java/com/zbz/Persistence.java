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
    public static int NUM = 10;
    public static int CHAR = 10;

    private FileChannel fc;
    private Table table;
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
        this.table = table;

        RECORD_WIDTH = 0;
        for(Byte type: table.getFields().values()) {
            if (type == Field.NUMERIC) {
                RECORD_WIDTH += Persistence.NUM;
            } else if (type == Field.STRING) {
                RECORD_WIDTH += Persistence.CHAR;
            }
        }
    }

    private void writeRecord(Record record, long offset) {
        try {
            ByteBuffer recordBytes = record.toBytes();
            ByteBuffer byteBuffer = ByteBuffer.allocate(RECORD_WIDTH);
            byteBuffer.put(recordBytes);
            fc.write(byteBuffer, offset);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public long insert(Record record) {
        writeRecord(record, recordOffset);
        long ret = recordOffset;
        recordOffset += RECORD_WIDTH;
        return ret;
    }

    public void update(Record record, long offset) {
        writeRecord(record, offset);
    }

    public Record query(long offset) {
        ByteBuffer recordBytes = ByteBuffer.allocate(RECORD_WIDTH);
        try {
            fc.read(recordBytes, offset);
            return Record.parseFromBytes(recordBytes, table);
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
