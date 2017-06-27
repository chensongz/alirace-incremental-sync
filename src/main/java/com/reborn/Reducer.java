package com.reborn;

import com.alibaba.middleware.race.sync.Server;
import gnu.trove.map.hash.TIntIntHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by bgk on 6/27/17.
 */
public class Reducer {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    private Server server;
    private int start;
    private int end;

    private TIntIntHashMap binlogHashMap = new TIntIntHashMap(DataConstants.HASHMAP_CAPACITY);
    private long[] fieldArray = new long[DataConstants.HASHMAP_CAPACITY * DataConstants.FIELD_COUNT];
    private int fieldArrayPosition = 0;

    private volatile int threadIndex = 0;

    private int[] powers = new int[]{1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000};
    private byte[] dataBuf = new byte[64];
    private int dataBufPosition;

    public Reducer(Server server, int start, int end) {
        this.server = server;
        this.start = start;
        this.end = end;
    }

    public boolean doReduce(Binlog[] binlogs, int row, int threadNumber) {
        if (threadNumber != threadIndex % DataConstants.PARSER_COUNT) {
            return false;
        }
        if (row == 0) {
            //end
            logger.info("reducer : last message");
            try {
                sendToClient();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return true;
        }

        Binlog currentBinlog;
        byte operation;
        int primaryValue;
        int primaryOldValue;
        int fieldLength;
        for (int i = 0; i < row; i++) {
            currentBinlog = binlogs[i];
            operation = currentBinlog.operation;
            fieldLength = currentBinlog.fieldLength;
            if (operation == 'I') {
                binlogHashMap.put(currentBinlog.primaryValue, fieldArrayPosition + 1);
                for (int j = 0; j < fieldLength; j++) {
                    putField(currentBinlog.fieldValues[j]);
                }
            } else if (operation == 'U') {
                primaryOldValue = currentBinlog.primaryOldValue;
                primaryValue = currentBinlog.primaryValue;

                int fieldHeaderIndex = binlogHashMap.get(primaryOldValue) - 1;
                for (int j = 0; j < fieldLength; j++) {
                    updateField(fieldHeaderIndex, currentBinlog.fieldNames[j], currentBinlog.fieldValues[j]);
                }

                if (primaryOldValue != primaryValue) {
                    binlogHashMap.remove(primaryOldValue);
                    binlogHashMap.put(primaryValue, fieldHeaderIndex + 1);
                }
            } else if (operation == 'D') {
                binlogHashMap.remove(currentBinlog.primaryOldValue);
            } else {
                logger.error("reducer: wrong operation !!");
            }
        }
        threadIndex++;
        return true;
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

    //methods for sending
    public void sendToClient() throws IOException {

        BufferedOutputStream sockStream;
        while ((sockStream = server.getBufferedClientStream()) == null) {
        }

        int sendCount = 0;
        for (int key = start + 1; key < end; key++) {
            int fieldHeadIndex = binlogHashMap.get(key) - 1;
            if (fieldHeadIndex >= 0) {
                doSend(key, fieldHeadIndex, sockStream);
                sendCount++;
            }
        }
        sockStream.write((byte) '\r');
        sockStream.flush();

        logger.info("send binlog count: " + sendCount);
    }

    public void doSend(int key, int fieldHeaderIndex, OutputStream sockStream) throws IOException {
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

    private void decode(long src, OutputStream sockStream) throws IOException {
        byte b;
        dataBufPosition = 0;
        while ((b = (byte) (src & 0xff)) != 0) {
            src >>= 8;
            dataBuf[dataBufPosition++] = b;
        }

        for (int i = dataBufPosition - 1; i >= 0; i--) {
            sockStream.write(dataBuf[i]);
        }
    }
}
