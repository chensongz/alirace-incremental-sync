package com.zbz;

/**
 * Created by bgk on 6/17/17.
 */

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.Server;
import gnu.trove.list.array.TByteArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class Reducer implements Runnable {
    private long start;
    private long end;
//    private Pool<String> sendPool;
    private TByteArrayList sendPool;

    private FieldIndex fieldIndex = new FieldIndex();
    private TLongObjectHashMap<long[]> binlogHashMap = new TLongObjectHashMap<>(DataConstans.HASHMAP_CAPACITY);

    private byte[] dataBuf = new byte[DataConstans.DATABUF_CAPACITY];
    private int position = 0;

    private StringBuilder sb = new StringBuilder(64);

    public Reducer(long start, long end, TByteArrayList sendPool) {
        this.start = start;
        this.end = end;
        this.sendPool = sendPool;
    }

    @Override
    public void run() {
        Logger logger = LoggerFactory.getLogger(Server.class);
        logger.info("Reducer run start!");
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < Constants.DATA_FILE_NUM; i++) {
            try {
                logger.info("Reduce " + Constants.getDataFile(i) + " start!");
                reduceDataFile(Constants.getDataFile(i));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        long t2 = System.currentTimeMillis();
        logger.info("reduce all data file cost: " + (t2 - t1) + " ms");
        int sendCount = 0;
        for (long key = start + 1; key < end; key++) {
            long[] fields = binlogHashMap.get(key);
            if (fields != null) {
                sendToPool(key, fields, sendPool);
                sendCount++;
            }
        }
        sendPool.add((byte)'\r');
        logger.info("send binlog count: " + sendCount);
    }

    private void reduceDataFile(String filename) throws IOException {
        Logger logger = LoggerFactory.getLogger(Server.class);

        FileChannel fc = new RandomAccessFile(filename, "r").getChannel();
        MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
        long t1 = System.currentTimeMillis();
        long primaryValue;
        long primaryOldValue;
        while (buffer.hasRemaining()) {
            skipLineUseless(buffer);
            byte operation = buffer.get();
            if (operation == 'I') {
                skip(buffer, DataConstans.ID_SIZE + DataConstans.NULL_SIZE);
                readUntilCharacter(buffer, dataBuf, DataConstans.SEPARATOR);
                primaryValue = ReduceUtils.bytes2Long(dataBuf, position);
                // until '\n'
                long[] fields = new long[DataConstans.FIELD_COUNT];
                while (readUntilCharacter(buffer, dataBuf, DataConstans.INNER_SEPARATOR)) {
                    int fieldName = sum();
                    if (!fieldIndex.isInit()) {
                        logger.info("field name sum: " + fieldName);
                        logger.info("field real name: " + new String(toByteArray()));
                        fieldIndex.put(fieldName);
                    }
                    skip(buffer, DataConstans.FIELD_TYPE_SIZE + DataConstans.NULL_SIZE);
                    readUntilCharacter(buffer, dataBuf, DataConstans.SEPARATOR);
                    long fieldValue = encode();
                    fields[fieldIndex.get(fieldName)] = fieldValue;
                }
                if (!fieldIndex.isInit()) {
                    fieldIndex.setInit(true);
                }
                binlogHashMap.put(primaryValue, fields);
            } else if (operation == 'U') {
                // skip |id:1:1|
                skip(buffer, DataConstans.ID_SIZE);
                // read primary old value
                readUntilCharacter(buffer, dataBuf, DataConstans.SEPARATOR);
                primaryOldValue = ReduceUtils.bytes2Long(dataBuf, position);
                // read primary value
                readUntilCharacter(buffer, dataBuf, DataConstans.SEPARATOR);
                primaryValue = ReduceUtils.bytes2Long(dataBuf, position);
                long[] fields = binlogHashMap.get(primaryOldValue);
                while (readUntilCharacter(buffer, dataBuf, DataConstans.INNER_SEPARATOR)) {
                    int fieldName = sum();
                    skip(buffer, DataConstans.FIELD_TYPE_SIZE);
                    skipUntilCharacter(buffer, DataConstans.SEPARATOR);
                    readUntilCharacter(buffer, dataBuf, DataConstans.SEPARATOR);
                    long fieldValue = encode();
                    fields[fieldIndex.get(fieldName)] = fieldValue;
                }

                if (primaryOldValue != primaryValue) {
                    binlogHashMap.remove(primaryOldValue);
                    binlogHashMap.put(primaryValue, fields);
                }
            } else if (operation == 'D') {
                // skip |id:1:1|
                skip(buffer, DataConstans.ID_SIZE);
                // read primary old value
                readUntilCharacter(buffer, dataBuf, DataConstans.SEPARATOR);
                primaryOldValue = ReduceUtils.bytes2Long(dataBuf, position);
                binlogHashMap.remove(primaryOldValue);
                skipUntilCharacter(buffer, DataConstans.LF);
            } else {
                logger.error("=== exception character ===");
            }
        }
        long t2 = System.currentTimeMillis();
        logger.info(filename + " size: " + fc.size());
        logger.info(filename + " reduce cost time: " + (t2 - t1) + " ms");
    }


    public void skipLineUseless(ByteBuffer byteBuffer) {
        // skip |mysql-bin.000017630680234|1496737946000|middleware3|student|
        skip(byteBuffer, DataConstans.BINARY_PRE_SIZE);
        skipUntilCharacter(byteBuffer, DataConstans.SEPARATOR);
        skip(byteBuffer, DataConstans.OTHER_PRE_SIZE);
    }

    public void skip(ByteBuffer byteBuffer, int skipCount) {
        byteBuffer.position(byteBuffer.position() + skipCount);
    }

    public void skipUntilCharacter(ByteBuffer byteBuffer, byte skipCharacter) {
        while (byteBuffer.hasRemaining() && byteBuffer.get() != skipCharacter) {
        }
    }

    public boolean readUntilCharacter(ByteBuffer byteBuffer, byte[] bao, byte skipCharacter) {
        reset();
        byte b;
        while (byteBuffer.hasRemaining()) {
            b = byteBuffer.get();
            if (b == DataConstans.LF) return false;
            if (b != skipCharacter) {
                write(b);
            } else {
                break;
            }
        }
        return true;
    }


    public void reset() {
        position = 0;
    }

    public void write(byte b) {
        dataBuf[position++] = b;
    }

    public byte[] toByteArray() {
        byte[] bytes = new byte[position];
        System.arraycopy(dataBuf, 0, bytes, 0, position);
        return bytes;
    }

    private int sum() {
        int sum = 0;
        for (int i = 0; i < position; i++) {
            sum += dataBuf[i];
        }
        return sum;
    }

    private long encode() {
        long result = 0;
        for (int i = position - 1; i >= 0; i--) {
            result <<= 8;
            result |= (dataBuf[i] & 0xff);
        }
        return result;
    }

    private void decode(long src) {
        // decode long to string
        byte b;
        reset();
        while ((b = (byte) (src & 0xff)) != 0) {
            write(b);
            src >>= 8;
        }
        sb.append(new String(dataBuf, 0, position));
    }

    private void decode(long src, TByteArrayList pool) {
        // decode long to string
        byte b;
        reset();
        while ((b = (byte) (src & 0xff)) != 0) {
            write(b);
            src >>= 8;
        }
        pool.add(dataBuf, 0, position);
    }

    public void sendToPool(long key, long[] fields, Pool<String> pool) {
        sb.setLength(0);
        sb.append(key).append("\t");
        for (int i = 0; i < fields.length; i++) {
            if (fields[i] != 0) {
                decode(fields[i]);
                sb.append("\t");
            }
        }
        sb.setLength(sb.length() - 1);
        sb.append("\n");
        pool.put(sb.toString());
    }

    public void sendToPool(long key, long[] fields, TByteArrayList pool) {
        long2Bytes(key);
        pool.add((byte)'\t');
        for(int i = 0; i < fields.length; i++) {
            if(fields[i] != 0) {
                decode(fields[i], pool);
                pool.add((byte)'\t');
            }
        }
        pool.remove(0,1);
        pool.add((byte)'\n');
    }

    public void long2Bytes(long src) {
        byte b;
        long x = src;
        long p;
        int len = 0;
        while (x > 0) {
            x /= 10;
            len++;
        }
        for (int i = len - 1; i >= 0; i--) {
            p = power(i);
            b = (byte)(src/p);
            src -= b * p;
            b += (byte)'0';
            sendPool.add(b);
        }
    }

    public long power(int num) {
        long ret = 1;
        while (num-- > 0) {
            ret *= 10;
        }
        return ret;
    }
}

