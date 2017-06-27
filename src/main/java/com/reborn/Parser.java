package com.reborn;

import com.alibaba.middleware.race.sync.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by bgk on 6/27/17.
 */
public class Parser implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);
    private int threadNumber;
    private RingBuffer ringBuffer;
    private Reducer reducer;

    private Binlog[] binlogs;
    private byte[] parseBuffer;
    private int position;
    private int dataLength;
    private byte[] lengthBuffer;

    public Parser(int threadNumber, RingBuffer ringBuffer, Reducer reducer) {
        this.threadNumber = threadNumber;
        this.ringBuffer = ringBuffer;
        this.reducer = reducer;
        this.binlogs = new Binlog[DataConstants.MAX_MESSAGE_COUNT];
        this.parseBuffer = new byte[DataConstants.READ_BUFFER_SIZE + DataConstants.MAX_MESSAGE_SIZE];
        this.position = 0;
        this.lengthBuffer = new byte[4];
        init();
    }

    public void init() {
        for (int i = 0; i < DataConstants.MAX_MESSAGE_COUNT; i++) {
            binlogs[i] = new Binlog();
        }
    }

    @Override
    public void run() {
        dataLength = 0;
        while (true) {
            while (ringBuffer.get(lengthBuffer, 4) == null) {
            }
            dataLength = bytes2Int(lengthBuffer);

            if (dataLength == 0) {
                while (!reducer.doReduce(null, 0, threadNumber)) {
                }
            } else {
                while (ringBuffer.get(parseBuffer, dataLength) == null) {
                }
                parse();
            }
        }
    }

    public void parse() {
        resetParseBuffer();
        int row = 0;
        while (position < dataLength) {
            parseRow(row++);
        }
        // reduce
        while (!reducer.doReduce(binlogs, row, threadNumber)) {
        }
    }

    public void resetParseBuffer() {
        position = 0;
    }

    public void parseRow(int row) {
        Binlog binlog = binlogs[row];
        int primaryValue;
        int primaryOldValue;
        skipLineUseless();
        byte operation = parseBuffer[position++];
        if (operation == 'I') {
            skip(DataConstants.ID_SIZE + DataConstants.NULL_SIZE);
            primaryValue = parsePrimaryValue();
            binlog.operation = operation;
            binlog.primaryValue = primaryValue;

            binlog.reset();
            while (true) {
                byte fieldName = parseInsertFieldIndex();
                if (fieldName == -1) break;
                skip(DataConstants.FIELD_TYPE_SIZE + DataConstants.NULL_SIZE);
                long fieldValue = encode();
                binlog.put(fieldName, fieldValue);
            }
        } else if (operation == 'U') {
            // skip |id:1:1|
            skip(DataConstants.ID_SIZE);
            primaryOldValue = parsePrimaryValue();
            primaryValue = parsePrimaryValue();
            binlog.operation = operation;
            binlog.primaryOldValue = primaryOldValue;
            binlog.primaryValue = primaryValue;
            binlog.reset();
            while (true) {
                byte fieldName = parseUpdateFieldIndex();
                if (fieldName == -1) break;
                long fieldValue = encode();
                binlog.put(fieldName, fieldValue);
            }
        } else if (operation == 'D') {
            // skip |id:1:1|
            skip(DataConstants.ID_SIZE);
            primaryOldValue = parsePrimaryValue();
            binlog.operation = operation;
            binlog.primaryOldValue = primaryOldValue;
            binlog.reset();
            skip(106);
//          skip(buffer, 87);  // local test
            skipUntilCharacter((byte) '\n');
        } else {
            logger.info("wrong operation !!");
        }
    }

    public void skipLineUseless() {
        // skip |mysql-bin.000017630680234|1496737946000|middleware3|student|
        skip(DataConstants.BINARY_PRE_SIZE);
        skipUntilCharacter(DataConstants.SEPARATOR);
        skip(DataConstants.OTHER_PRE_SIZE);
    }

    public void skip(int skipLength) {
        position += skipLength;
    }

    public void skipUntilCharacter(byte skipCharacter) {
        while (position < dataLength && parseBuffer[position++] != skipCharacter) {
        }
    }

    public int parsePrimaryValue() {
        int result = 0;
        byte b;
        while ((b = parseBuffer[position++]) != DataConstants.SEPARATOR) {
            result = (result << 3) + (result << 1) + (b - '0');
        }
        return result;
    }

    public byte parseInsertFieldIndex() {
        byte b = parseBuffer[position++];
        if (b == '\n') return -1;
        if (b == 'f') {
            skip(10);
            return 0;
        } else if (b == 'l') {
            skip(9);
            return 1;
        } else if (b == 's') {
            b = parseBuffer[position++];
            if (b == 'e') {
                skip(2);
                return 2;
            } else {
                skip(3);
                if (parseBuffer[position++] == ':') {
                    return 3;
                } else {
                    skip(1);
                    return 4;
                }
            }
        }
        return -1;
    }

    public byte parseUpdateFieldIndex() {
        byte b = parseBuffer[position++];
        if (b == '\n') return -1;
        if (b == 'f') {
            skip(18);
            return 0;
        } else if (b == 'l') {
            skip(16);
            skipUntilCharacter(DataConstants.SEPARATOR);
            return 1;
        } else if (b == 's') {
            b = parseBuffer[position++];
            if (b == 'e') {
                skip(10);
                return 2;
            } else {
                skip(3);
                if (parseBuffer[position++] == ':') {
                    skip(6);
                    skipUntilCharacter(DataConstants.SEPARATOR);
                    return 3;
                } else {
                    skip(7);
                    skipUntilCharacter(DataConstants.SEPARATOR);
                    return 4;
                }
            }
        }
        return -1;
    }

    public long encode() {
        long result = 0;
        byte b;
        while (true) {
            b = parseBuffer[position++];
            if (b != DataConstants.SEPARATOR) {
                result <<= 8;
                result |= (b & 0xff);
            } else {
                break;
            }
        }
        return result;
    }

    public int bytes2Int(byte[] bytes) {
        return bytes[3] & 0xFF |
                (bytes[2] & 0xFF) << 8 |
                (bytes[1] & 0xFF) << 16 |
                (bytes[0] & 0xFF) << 24;
    }
}
