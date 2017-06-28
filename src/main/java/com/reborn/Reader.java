package com.reborn;

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by bgk on 6/27/17.
 */
public class Reader implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    private RingBuffer[] ringBuffers;

    private int ringBufferIndex = 0;
    private long spin;

    public Reader(RingBuffer[] ringBuffers) {
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
        //end flag
        RingBuffer currentRingBuffer = ringBuffers[ringBufferIndex % DataConstants.PARSER_COUNT];
        while (!currentRingBuffer.put(ByteBuffer.allocate(0))) {
        }
        ringBufferIndex++;
        long t2 = System.currentTimeMillis();
        logger.info("read all data file cost: " + (t2 - t1) + " ms");
    }


    private void readAndDispatch(String filename) throws IOException {
        FileChannel fc = new RandomAccessFile(filename, "r").getChannel();
        int size = (int) fc.size();
        int offset = 0;
        MappedByteBuffer buffer = null;

        long t1 = System.currentTimeMillis();

        boolean readFlag = true;
        spin = 0;
        while (readFlag) {
            int remaining = size - offset;
            if (remaining >= DataConstants.MAPSIZE) {
                buffer = fc.map(FileChannel.MapMode.READ_ONLY, offset, DataConstants.MAPSIZE);
                offset += backToLF(buffer);
            } else {
                if (remaining == 0) {
                    break;
                } else {
                    buffer = fc.map(FileChannel.MapMode.READ_ONLY, offset, remaining);
                    readFlag = false;
                }
            }
            RingBuffer currentRingBuffer = ringBuffers[ringBufferIndex % DataConstants.PARSER_COUNT];
            while (!currentRingBuffer.put(buffer)) {
                spin++;
            }
            ringBufferIndex++;
        }

        long t2 = System.currentTimeMillis();
        logger.info(filename + " size: " + fc.size());
        logger.info(filename + " read cost time: " + (t2 - t1) + " ms");
        logger.info(filename + " spin: " + spin);
    }

    public int backToLF(MappedByteBuffer mappedByteBuffer) {
        int mmbSize = mappedByteBuffer.limit();
        mappedByteBuffer.position(mmbSize - DataConstants.MAX_MESSAGE_SIZE);
        while (mappedByteBuffer.get() != '\n') {
        }
        int mmbLimit = mappedByteBuffer.position();
        mappedByteBuffer.position(0);
        mappedByteBuffer.limit(mmbLimit);
        return mmbLimit;
    }
}
