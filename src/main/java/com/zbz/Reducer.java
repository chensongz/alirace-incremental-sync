package com.zbz;

/**
 * Created by bgk on 6/17/17.
 */

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.Server;
import gnu.trove.map.hash.TIntIntHashMap;
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
    private int start;
    private int end;

    private TIntIntHashMap binlogHashMap = new TIntIntHashMap(DataConstants.HASHMAP_CAPACITY);

    private long[] fieldArray = new long[DataConstants.INSERT_CAPACITY * DataConstants.FIELD_COUNT];
    private int fieldArrayPosition = 0;

    private byte[] dataBuf = new byte[DataConstants.DATABUF_CAPACITY];
    private int position = 0;

    private int[] powers = new int[]{1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000};

    public Reducer(int start, int end) {
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
        for (int key = start + 1; key < end; key++) {
            int fieldHeadIndex = binlogHashMap.get(key) - 1;
            if (fieldHeadIndex >= 0) {
                sendToPool(key, fieldHeadIndex, sockStream);
                sendCount++;
            }
        }
        sockStream.write((byte) '\r');
        logger.info("send binlog count: " + sendCount);
    }

    private void reduceDataFile(String filename) throws IOException {
        Logger logger = LoggerFactory.getLogger(Server.class);

        FileChannel fc = new RandomAccessFile(filename, "r").getChannel();
        int size = (int) fc.size();
        MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, 0, size);

        long t1 = System.currentTimeMillis();
        int primaryValue;
        int primaryOldValue;

        while (buffer.position() < size) {
            skipLineUseless(buffer, size);
            byte operation = buffer.get();
            if (operation == 'I') {
                skip(buffer, DataConstants.ID_SIZE + DataConstants.NULL_SIZE);
                primaryValue = bytes2Int(buffer);
                // until '\n'
                binlogHashMap.put(primaryValue, fieldArrayPosition + 1);

                while (true) {
                    byte fieldName = sum(buffer, size);
                    if (fieldName == -1) break;

                    skip(buffer, DataConstants.FIELD_TYPE_SIZE + DataConstants.NULL_SIZE);
                    long fieldValue = encode(buffer, size);
                    putField(fieldValue);
                }

            } else if (operation == 'U') {
                // skip |id:1:1|
                skip(buffer, DataConstants.ID_SIZE);
                // read primary old value
                primaryOldValue = bytes2Int(buffer);
                // read primary value
                primaryValue = bytes2Int(buffer);
                int fieldHeaderIndex = binlogHashMap.get(primaryOldValue) - 1;

                while (true) {
                    byte fieldName = sum2(buffer, size);
                    if (fieldName == -1) break;
                    long fieldValue = encode(buffer, size);
                    updateField(fieldHeaderIndex, fieldName, fieldValue);
                }

                if (primaryOldValue != primaryValue) {
                    binlogHashMap.remove(primaryOldValue);
                    binlogHashMap.put(primaryValue, fieldHeaderIndex + 1);
                }
            } else if (operation == 'D') {
                // skip |id:1:1|
                skip(buffer, DataConstants.ID_SIZE);
                // read primary old value
                primaryOldValue = bytes2Int(buffer);
                binlogHashMap.remove(primaryOldValue);
//                skip(buffer, 106);
                skip(buffer, 87);
                skipUntilCharacter(buffer, DataConstants.LF, size);
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
        skip(byteBuffer, DataConstants.BINARY_PRE_SIZE);
        skipUntilCharacter(byteBuffer, DataConstants.SEPARATOR, size);
        skip(byteBuffer, DataConstants.OTHER_PRE_SIZE);
    }

    public void skip(ByteBuffer byteBuffer, int skipCount) {
        byteBuffer.position(byteBuffer.position() + skipCount);
    }

    public void skipUntilCharacter(ByteBuffer byteBuffer, byte skipCharacter, int size) {
        while (byteBuffer.position() < size && byteBuffer.get() != skipCharacter) {
        }
    }

    public byte sum(ByteBuffer byteBuffer, int size) {
        byte b = byteBuffer.get();
        if (b == '\n') return -1;
        if (b == 'f') {
            skip(byteBuffer, 10);
            return 0;
        } else if (b == 'l') {
            skip(byteBuffer, 9);
            return 1;
        } else if (b == 's') {
            b = byteBuffer.get();
            if (b == 'e') {
                skip(byteBuffer, 2);
                return 2;
            } else {
                skip(byteBuffer, 3);
                if (byteBuffer.get() == ':') {
                    return 3;
                } else {
                    skip(byteBuffer, 1);
                    return 4;
                }
            }
        }
        return -1;
    }

    public byte sum2(ByteBuffer byteBuffer, int size) {
        byte b = byteBuffer.get();
        if (b == '\n') return -1;
        if (b == 'f') {
            skip(byteBuffer, 18);
            return 0;
        } else if (b == 'l') {
            skip(byteBuffer, 16);
            skipUntilCharacter(byteBuffer, (byte)'|', size);
            return 1;
        } else if (b == 's') {
            b = byteBuffer.get();
            if (b == 'e') {
                skip(byteBuffer, 10);
                return 2;
            } else {
                skip(byteBuffer, 3);
                if (byteBuffer.get() == ':') {
                    skip(byteBuffer, 6);
                    skipUntilCharacter(byteBuffer, (byte)'|', size);
                    return 3;
                } else {
                    skip(byteBuffer, 7);
                    skipUntilCharacter(byteBuffer, (byte)'|', size);
                    return 4;
                }
            }
        }
        return -1;
    }

    public int bytes2Int(ByteBuffer byteBuffer) {
        int result = 0;
        byte b;
        while ((b = byteBuffer.get()) != DataConstants.SEPARATOR) {
            result = (result << 3) + (result << 1) + (b - '0');
        }
        return result;
    }

    public void reset() {
        position = 0;
    }

    public void write(byte b) {
        dataBuf[position++] = b;
    }


    public long encode(ByteBuffer byteBuffer, int size) {
        long result = 0;
        byte b;
        while (true) {
            b = byteBuffer.get();
            if (b != DataConstants.SEPARATOR) {
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


    public void sendToPool(int key, int fieldHeaderIndex, OutputStream sockStream) throws IOException {
        int2Bytes(key, sockStream);
        sockStream.write((byte) '\t');

        int idxCount = DataConstants.FIELD_COUNT;
        for (int i = 0; i < idxCount; i++) {
            decode(getField(fieldHeaderIndex, (byte) i), sockStream);
            if (i < idxCount - 1) {
                sockStream.write((byte) '\t');
            }
        }

        sockStream.write((byte) '\n');
    }


    public void int2Bytes(int src, OutputStream sockStream) throws IOException {
        byte b;
        int x = src;
        int p;
        int len = 0;
        while (x > 0) {
            x /= 10;
            len++;
        }
        for (int i = len - 1; i >= 0; i--) {
            p = powers[i];
            b = (byte) (src / p);
            src -= b * p;
            b += (byte) '0';
            sockStream.write(b);
        }
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

