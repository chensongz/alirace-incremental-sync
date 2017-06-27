package com.reborn;

import com.alibaba.middleware.race.sync.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by bgk on 6/27/17.
 */
public class Parser implements Runnable{
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
    }

    @Override
    public void run() {
        dataLength = 0;
        while(true) {
            while(ringBuffer.get(lengthBuffer, 4) == null) {}
            dataLength = bytes2Int(lengthBuffer);
            while(ringBuffer.get(parseBuffer, dataLength) == null) {}

        }
    }

    public int bytes2Int(byte[] bytes) {
        return   bytes[3] & 0xFF |
                (bytes[2] & 0xFF) << 8 |
                (bytes[1] & 0xFF) << 16 |
                (bytes[0] & 0xFF) << 24;
    }

    public void parse() {
        reset();
        while(position < dataLength) {

        }
    }

    public void reset() {
        position = 0;
    }

    public void parseRow() {
        int primaryValue;
        int primaryOldValue;
        skipLineUseless();
        byte operation = parseBuffer[position++];
        if(operation == 'I') {
            skip(DataConstants.ID_SIZE + DataConstants.NULL_SIZE);
            primaryValue = bytes2Int();
            while(true) {
                //todo parse fields
            }
            //todo merge
        } else if (operation == 'U') {
            // skip |id:1:1|
            skip(DataConstants.ID_SIZE);
            primaryOldValue = bytes2Int();
            primaryValue = bytes2Int();

            while(true) {
                //todo parse fields
            }
            //todo merge

        } else if (operation == 'D') {
            // skip |id:1:1|
            skip(DataConstants.ID_SIZE);
            primaryOldValue = bytes2Int();
            //todo merge
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

    public int bytes2Int() {
        int result = 0;
        byte b;
        while ((b = parseBuffer[position++]) != DataConstants.SEPARATOR) {
            result = (result << 3) + (result << 1) + (b - '0');
        }
        return result;
    }
}
