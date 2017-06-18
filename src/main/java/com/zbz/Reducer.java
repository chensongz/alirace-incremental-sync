package com.zbz;

/**
 * Created by bgk on 6/17/17.
 */

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.Server;
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
    private Pool<String> sendPool;

    private FieldIndex fieldIndex = new FieldIndex();
    private TLongObjectHashMap<byte[][]> binlogHashMap = new TLongObjectHashMap<>(DataConstans.HASHMAP_CAPACITY);

    private byte[] dataBuf = new byte[DataConstans.DATABUF_CAPACITY];
    private int position = 0;

    public Reducer(long start, long end, Pool<String> sendPool) {
        this.start = start;
        this.end = end;
        this.sendPool = sendPool;
    }

    @Override
    public void run() {
        Logger logger = LoggerFactory.getLogger(Server.class);
        logger.info("Reducer run start");
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < Constants.DATA_FILE_NUM; i++) {
            try {
                logger.info("Reduce " + Constants.getDataFile(i) + " start!");
                reduceDataFile(Constants.getDataFile(i));
                logger.info("Reduce " + Constants.getDataFile(i) + " end!");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        long t2 = System.currentTimeMillis();
        logger.info("reduce data file cost:" + (t2 - t1));
        int sendCount = 0;
        for (long key = start + 1; key < end; key++) {
            byte[][] fields = binlogHashMap.get(key);
            if (fields != null) {
                sendPool.put(toSendString(key, fields));
                sendCount++;
            }
        }
        sendPool.put("NULL");
        logger.info("send binlog count: " + sendCount);
    }

    private void reduceDataFile(String filename) throws IOException {
        Logger logger = LoggerFactory.getLogger(Server.class);

        FileChannel fc = new RandomAccessFile(filename, "r").getChannel();
        MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
        logger.info("reduceDataFile:" + filename);
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
                byte[][] fields = new byte[DataConstans.FIELD_COUNT][];
                while (readUntilCharacter(buffer, dataBuf, DataConstans.INNER_SEPARATOR)) {
                    byte fieldName = dataBuf[1];
                    if (!fieldIndex.isInit()) {
                        logger.info("filename :" + fieldName);
                        fieldIndex.put(fieldName);
                    }
                    skip(buffer, DataConstans.FIELD_TYPE_SIZE + DataConstans.NULL_SIZE);
                    readUntilCharacter(buffer, dataBuf, DataConstans.SEPARATOR);
                    byte[] fieldValue = toByteArray();
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
                byte[][] fields = binlogHashMap.get(primaryOldValue);
                while (readUntilCharacter(buffer, dataBuf, DataConstans.INNER_SEPARATOR)) {
                    byte fieldName = dataBuf[1];
                    skip(buffer, DataConstans.FIELD_TYPE_SIZE);
                    skipUntilCharacter(buffer, DataConstans.SEPARATOR);
                    readUntilCharacter(buffer, dataBuf, DataConstans.SEPARATOR);
                    byte[] fieldValue = toByteArray();
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
                logger.warn("exception character");
            }
        }
        long t2 = System.currentTimeMillis();
        logger.info(filename + " size: " + fc.size());
        logger.info(filename + " MappedByteBuffer read byte cost time: " + (t2 - t1) + " ms");
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

    public String toSendString(long key, byte[][] bytes) {
        StringBuilder sb = new StringBuilder(32);
        sb.append(key).append("\t");
        for (int i = 0; i < bytes.length; i++) {
            sb.append(new String(bytes[i])).append("\t");
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

}

