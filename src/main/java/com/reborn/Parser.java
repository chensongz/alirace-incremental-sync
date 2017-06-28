package com.reborn;

import com.alibaba.middleware.race.sync.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

/**
 * Created by bgk on 6/27/17.
 */
public class Parser implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);
    private int threadNumber;
    private RingBuffer ringBuffer;
    private Reducer reducer;

    private Binlog[] binlogs;

    public Parser(int threadNumber, RingBuffer ringBuffer, Reducer reducer) {
        this.threadNumber = threadNumber;
        this.ringBuffer = ringBuffer;
        this.reducer = reducer;
        this.binlogs = new Binlog[DataConstants.MAX_MESSAGE_COUNT];
        init();
    }

    public void init() {
        for (int i = 0; i < DataConstants.MAX_MESSAGE_COUNT; i++) {
            binlogs[i] = new Binlog();
        }
    }

    @Override
    public void run() {
        while (true) {
            ByteBuffer byteBuffer ;
            while ((byteBuffer = ringBuffer.get()) == null) {
            }
            int limit = byteBuffer.limit();
            if (limit == 0) {
                while (!reducer.doReduce(null, 0, threadNumber)) {
                }
            } else {
                parse(byteBuffer, limit);
            }
        }
    }

    public void parse(ByteBuffer byteBuffer, int limit) {
        int row = 0;
        while (byteBuffer.position() < limit) {
            parseRow(byteBuffer, row++);
        }
        // reduce
        while (!reducer.doReduce(binlogs, row, threadNumber)) {
        }
    }


    public void parseRow(ByteBuffer byteBuffer, int row) {
        Binlog binlog = binlogs[row];
        int primaryValue;
        int primaryOldValue;
        skipLineUseless(byteBuffer);
        byte operation = byteBuffer.get();
        if (operation == 'I') {
            skip(byteBuffer, DataConstants.ID_SIZE + DataConstants.NULL_SIZE);
            primaryValue = parsePrimaryValue(byteBuffer);
            binlog.operation = operation;
            binlog.primaryValue = primaryValue;

            binlog.reset();
            while (true) {
                byte fieldName = parseInsertFieldIndex(byteBuffer);
                if (fieldName == -1) break;
                skip(byteBuffer, DataConstants.FIELD_TYPE_SIZE + DataConstants.NULL_SIZE);
                long fieldValue = encode(byteBuffer);
                binlog.put(fieldName, fieldValue);
            }
        } else if (operation == 'U') {
            // skip |id:1:1|
            skip(byteBuffer, DataConstants.ID_SIZE);
            primaryOldValue = parsePrimaryValue(byteBuffer);
            primaryValue = parsePrimaryValue(byteBuffer);
            binlog.operation = operation;
            binlog.primaryOldValue = primaryOldValue;
            binlog.primaryValue = primaryValue;
            binlog.reset();
            while (true) {
                byte fieldName = parseUpdateFieldIndex(byteBuffer);
                if (fieldName == -1) break;
                long fieldValue = encode(byteBuffer);
                binlog.put(fieldName, fieldValue);
            }
        } else if (operation == 'D') {
            // skip |id:1:1|
            skip(byteBuffer, DataConstants.ID_SIZE);
            primaryOldValue = parsePrimaryValue(byteBuffer);
            binlog.operation = operation;
            binlog.primaryOldValue = primaryOldValue;
            binlog.reset();
            skip(byteBuffer, DataConstants.DELETE_SKIP_COUNT);
            skipUntilCharacter(byteBuffer, (byte) '\n');
        } else {
            logger.info("wrong operation !! " + (char)operation);
        }
    }

    public void skipLineUseless(ByteBuffer byteBuffer) {
//         skip |mysql-bin.000017630680234|1496737946000|middleware3|student|
        skip(byteBuffer, DataConstants.BINARY_PRE_SIZE);
        skipUntilCharacter(byteBuffer, DataConstants.SEPARATOR);
        skip(byteBuffer, DataConstants.OTHER_PRE_SIZE);
    }

    public void skip(ByteBuffer byteBuffer, int skipLength) {
        byteBuffer.position(byteBuffer.position() + skipLength);
    }

    public void skipUntilCharacter(ByteBuffer byteBuffer, byte skipCharacter) {
        while (byteBuffer.get() != skipCharacter) {
        }
    }
//
    public int parsePrimaryValue(ByteBuffer byteBuffer) {
        int result = 0;
        byte b;
        while ((b = byteBuffer.get()) != DataConstants.SEPARATOR) {
            result = (result << 3) + (result << 1) + (b - '0');
        }
        return result;
    }

    public byte parseInsertFieldIndex(ByteBuffer byteBuffer) {
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

    public byte parseUpdateFieldIndex(ByteBuffer byteBuffer) {
        byte b = byteBuffer.get();
        if (b == '\n') return -1;
        if (b == 'f') {
            skip(byteBuffer, 18);
            return 0;
        } else if (b == 'l') {
            skip(byteBuffer, 16);
            skipUntilCharacter(byteBuffer, DataConstants.SEPARATOR);
            return 1;
        } else if (b == 's') {
            b = byteBuffer.get();
            if (b == 'e') {
                skip(byteBuffer, 10);
                return 2;
            } else {
                skip(byteBuffer, 3);
                if (byteBuffer.get() == ':') {
                    skip(byteBuffer,6);
                    skipUntilCharacter(byteBuffer, DataConstants.SEPARATOR);
                    return 3;
                } else {
                    skip(byteBuffer, 7);
                    skipUntilCharacter(byteBuffer, DataConstants.SEPARATOR);
                    return 4;
                }
            }
        }
        return -1;
    }

    public long encode(ByteBuffer byteBuffer) {
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

}
