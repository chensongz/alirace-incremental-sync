package com.zbz;

/**
 * Created by bgk on 6/17/17.
 */

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.Server;
import gnu.trove.map.hash.TLongIntHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class Reducer implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);
    private long start;
    private long end;

//    private FieldIndex fieldIndex = new FieldIndex();
    private TLongIntHashMap binlogHashMap = new TLongIntHashMap(DataConstans.HASHMAP_CAPACITY);

    private long[] fieldArray = new long[DataConstans.INSERT_CAPACITY * DataConstans.FIELD_COUNT];
    private int fieldArrayPosition = 0;

    private byte[] dataBuf = new byte[DataConstans.DATABUF_CAPACITY];
    private int position = 0;

    private byte[] fieldIdx = new byte[11];
    private byte fieldPosition = 0;
    private boolean isInit = false;

    public Reducer(long start, long end) {
        this.start = start;
        this.end = end;
    }

    @Override
    public void run() {
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
    }

    public void sendToSocketDirectly(OutputStream sockStream) throws IOException {
        int sendCount = 0;
        for (long key = start + 1; key < end; key++) {
            int fieldHeadIndex = binlogHashMap.get(key) - 1;
            if (fieldHeadIndex >= 0) {
                sendToPool(key, fieldHeadIndex, sockStream);
                sendCount++;
            }
        }
        sockStream.write((byte)'\r');
        logger.info("send binlog count: " + sendCount);
    }

    private void reduceDataFile(String filename) throws IOException {
        Logger logger = LoggerFactory.getLogger(Server.class);

        FileChannel fc = new RandomAccessFile(filename, "r").getChannel();
        int size = (int)fc.size();
        MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, 0, size);

        long t1 = System.currentTimeMillis();
        long primaryValue;
        long primaryOldValue;

        boolean isInit = this.isInit;

        while (buffer.position() < size) {
            skipLineUseless(buffer, size);
            byte operation = buffer.get();
            if (operation == 'I') {
                skip(buffer, DataConstans.ID_SIZE + DataConstans.NULL_SIZE);
                readUntilCharacter(buffer, DataConstans.SEPARATOR, size);
                primaryValue = ReduceUtils.bytes2Long(dataBuf, position);
                // until '\n'
                binlogHashMap.put(primaryValue, fieldArrayPosition + 1);


                while(true) {
                    int fieldName = sum(buffer, size);
                    if(fieldName == 0) break;
                    if (!isInit) {
//                        logger.info("field name sum: " + fieldName);
//                        logger.info("field real name: " + new String(toByteArray()));
//                        fieldIndex.put(fieldName);
                        this.fieldIdx[fieldName] = fieldPosition++;
                    }
                    skip(buffer, DataConstans.FIELD_TYPE_SIZE + DataConstans.NULL_SIZE);
                    long fieldValue = encode(buffer, size);
                    if (fieldValue == 0) break;
                    putField(fieldValue);
                }
                if (!isInit) {
//                    fieldIndex.setInit(true);
                    this.isInit = true;
                    isInit = true;
                }
            } else if (operation == 'U') {
                // skip |id:1:1|
                skip(buffer, DataConstans.ID_SIZE);
                // read primary old value
                readUntilCharacter(buffer, DataConstans.SEPARATOR, size);
                primaryOldValue = ReduceUtils.bytes2Long(dataBuf, position);
                // read primary value
                readUntilCharacter(buffer, DataConstans.SEPARATOR, size);
                primaryValue = ReduceUtils.bytes2Long(dataBuf, position);
                int fieldHeaderIndex = binlogHashMap.get(primaryOldValue) - 1;

                while(true) {
                    int fieldName = sum(buffer, size);
                    if(fieldName == 0) break;
                    skip(buffer, DataConstans.FIELD_TYPE_SIZE);
                    skipUntilCharacter(buffer, DataConstans.SEPARATOR, size);
                    long fieldValue = encode(buffer, size);
                    if (fieldValue == 0) break;
                    updateField(fieldHeaderIndex, fieldIdx[fieldName], fieldValue);
                }


                if (primaryOldValue != primaryValue) {
                    binlogHashMap.remove(primaryOldValue);
                    binlogHashMap.put(primaryValue, fieldHeaderIndex + 1);
                }
            } else if (operation == 'D') {
                // skip |id:1:1|
                skip(buffer, DataConstans.ID_SIZE);
                // read primary old value
                readUntilCharacter(buffer, DataConstans.SEPARATOR, size);
                primaryOldValue = ReduceUtils.bytes2Long(dataBuf, position);
                binlogHashMap.remove(primaryOldValue);
                skipUntilCharacter(buffer, DataConstans.LF, size);
            } else {
                logger.error("=== exception character ===");
            }
        }
        long t2 = System.currentTimeMillis();
        logger.info(filename + " size: " + fc.size());
        logger.info(filename + " reduce cost time: " + (t2 - t1) + " ms");
    }


    public void skipLineUseless(ByteBuffer byteBuffer, int size) {
        // skip |mysql-bin.000017630680234|1496737946000|middleware3|student|
        skip(byteBuffer, DataConstans.BINARY_PRE_SIZE);
        skipUntilCharacter(byteBuffer, DataConstans.SEPARATOR, size);
        skip(byteBuffer, DataConstans.OTHER_PRE_SIZE);
    }

    public void skip(ByteBuffer byteBuffer, int skipCount) {
        byteBuffer.position(byteBuffer.position() + skipCount);
    }

    public void skipUntilCharacter(ByteBuffer byteBuffer, byte skipCharacter, int size) {
        while (byteBuffer.position() < size && byteBuffer.get() != skipCharacter) {
        }
    }

    public int sum(ByteBuffer byteBuffer, int size) {
        int sum = 0;
        byte b;
        while (byteBuffer.position() < size) {
            b = byteBuffer.get();
            if (b == DataConstans.LF) return 0;
            if (b != DataConstans.INNER_SEPARATOR) {
                sum++;
            } else {
                break;
            }
        }
        return sum;
    }

    public boolean readUntilCharacter(ByteBuffer byteBuffer, byte skipCharacter, int size) {
        reset();
        byte b;
        while (byteBuffer.position() < size) {
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


    public long encode(ByteBuffer byteBuffer, int size) {
        long result = 0;
        byte b;
        while (byteBuffer.position() < size) {
            b = byteBuffer.get();
            if (b == DataConstans.LF) return 0;
            if (b != DataConstans.SEPARATOR) {
                result <<= 8;
                result |= (b & 0xff);
            } else {
                break;
            }
        }
        return result;
    }


    private void decode(long src, OutputStream sockStream) throws IOException {
        byte b;
        reset();
        while ((b = (byte) (src & 0xff)) != 0) {
            src >>= 8;
            write(b);
        }

        for (int i = position - 1; i >= 0; i--) {
            sockStream.write(dataBuf[i]);
        }
    }


    public void sendToPool(long key, int fieldHeaderIndex, OutputStream sockStream) throws IOException{
        long2Bytes(key, sockStream);
        sockStream.write((byte)'\t');

        int idxCount = fieldPosition;
        for (int i = 0; i < idxCount; i++) {
            decode(getField(fieldHeaderIndex, (byte)i), sockStream);
            if (i < idxCount - 1) {
                sockStream.write((byte)'\t');
            }
        }

        sockStream.write((byte)'\n');
    }


    public void long2Bytes(long src, OutputStream sockStream) throws IOException{
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
            sockStream.write(b);
        }
    }

    public long power(int num) {
        long ret = 1;
        while (num-- > 0) {
            ret *= 10;
        }
        return ret;
    }

    public void putField(long fieldValue) {
        fieldArray[fieldArrayPosition++] = fieldValue;
    }

    public long getField(int fieldHeaderIndex, byte fieldIndex) {
        return fieldArray[fieldHeaderIndex + fieldIndex];
    }

    public void updateField(int fieldHeaderIndex, byte fieldIndex, long newFieldValue) {
        fieldArray[fieldHeaderIndex + fieldIndex] = newFieldValue;
    }
}

