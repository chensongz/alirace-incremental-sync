package com.zbz.bgk;

/**
 * Created by bgk on 6/17/17.
 */

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.Server;
import com.zbz.DataConstants;
import gnu.trove.map.hash.TIntIntHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class Reader<T> implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);
    private static final int ringBufferRemain = DataConstants.PARSER_COUNT - 1;

    private RingBuffer<byte[]>[] ringBuffers;
    public Reader(RingBuffer<byte[]>[] ringBuffers) {
        this.ringBuffers = ringBuffers;
    }

    @Override
    public void run() {
        logger.info("read run start!");
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < Constants.DATA_FILE_NUM; i++) {
            try {
                logger.info("read " + Constants.getDataFile(i) + " start!");
                readAndDispatch(Constants.getDataFile(i));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        long t2 = System.currentTimeMillis();
        logger.info("reduce all data file cost: " + (t2 - t1) + " ms");
    }


    private void readAndDispatch(String filename) throws IOException {

        FileChannel fc = new RandomAccessFile(filename, "r").getChannel();
        int size = (int) fc.size();
        MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, 0, size);

        long t1 = System.currentTimeMillis();
        int beginPosition;
        int endPosition = 0;
        int lineCount = 0;
        int ringBufferIndex;
        while (endPosition < size) {

            beginPosition = skipLineUseless(buffer, size);

            endPosition = skipUntilCharacter(buffer, DataConstants.LF, size);

            byte[] value = new byte[endPosition - beginPosition];
            buffer.position(beginPosition);
            buffer.get(value);

            ringBufferIndex = lineCount++ & ringBufferRemain;

            // put to ring buffer
            while(ringBuffers[ringBufferIndex].put(value) == null) {
            }


        }
        long t2 = System.currentTimeMillis();
        logger.info(filename + " size: " + fc.size());
        logger.info(filename + " reduce cost time: " + (t2 - t1) + " ms");
    }


    public int skipLineUseless(ByteBuffer byteBuffer, int size) {
        // skip |mysql-bin.000017630680234|1496737946000|middleware3|student|
        skip(byteBuffer, DataConstants.BINARY_PRE_SIZE);
        skipUntilCharacter(byteBuffer, DataConstants.SEPARATOR, size);
        return skip(byteBuffer, DataConstants.OTHER_PRE_SIZE);
    }

    public int skip(ByteBuffer byteBuffer, int skipCount) {
        int newPosition = byteBuffer.position() + skipCount;
        byteBuffer.position(newPosition);
        return newPosition;
    }

    public int skipUntilCharacter(ByteBuffer byteBuffer, byte skipCharacter, int size) {
        while (byteBuffer.position() < size && byteBuffer.get() != skipCharacter) {
        }
        return byteBuffer.position();
    }

}

