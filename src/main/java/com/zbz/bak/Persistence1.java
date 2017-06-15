package com.zbz.bak;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedHashMap;

/**
 * Created by zwy on 17-6-10.
 */
public class Persistence1 {
    public static int NUM = 10;
    public static int CHAR = 10;
    public static String SEPARATOR = "\t";

    private FileChannel fc;
    private Table table;
    private long recordOffset;
    private int RECORD_WIDTH;

    public Persistence1(String filename) {
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
                RECORD_WIDTH += Persistence1.NUM;
            } else if (type == Field.STRING) {
                RECORD_WIDTH += Persistence1.CHAR;
            }
        }
    }

    private void writeRecord(Record record, long offset) {
        try {
            byte[] recordBytes = record.toBytes();
            ByteBuffer byteBuffer = ByteBuffer.allocate(RECORD_WIDTH);
//            System.out.println("RRR " + record.toString() + ":" + recordBytes.length + ":" + RECORD_WIDTH);
            byteBuffer.put(recordBytes);
            byteBuffer.put((byte)'\r');
            byteBuffer.flip();
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
            return recordFromBytes(recordBytes);
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

    private Record recordFromString(String str) {
        String[] vals = str.split(SEPARATOR);
        LinkedHashMap<String, Byte> fields = table.getFields();
        Record ret = new Record();

        int i = 0;
        for(String field: fields.keySet()) {
            ret.put(field, vals[i++], table.isPrimaryKey(field));
        }
        return ret;
    }

    private Record recordFromBytes(ByteBuffer recordBytes) {
        recordBytes.flip();
        ByteArrayOutputStream bao = new ByteArrayOutputStream();

        while(recordBytes.remaining() > 0) {
            byte curr = recordBytes.get();
            if(curr != (byte)'\r') {
                bao.write(curr);
            } else {
                break;
            }
        }
        String recordString = bao.toString();
//        System.out.println("RecordFromBytes: " + recordString);
        return recordFromString(recordString);
    }
}
