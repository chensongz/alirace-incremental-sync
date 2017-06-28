package com.reborn;

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by bgk on 6/27/17.
 */
public class Reader implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    private byte[] readBuffer;
    private RingBuffer[] ringBuffers;

    private int ringBufferIndex = 0;
    private long spin;

    public Reader(RingBuffer[] ringBuffers) {
        this.ringBuffers = ringBuffers;
        readBuffer = new byte[DataConstants.READ_BUFFER_SIZE + DataConstants.MAX_MESSAGE_SIZE];
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
        setLength(readBuffer, 0);
        while (!ringBuffers[ringBufferIndex % DataConstants.PARSER_COUNT].put(readBuffer, 4)) {
        }
        ringBufferIndex++;
        long t2 = System.currentTimeMillis();
        logger.info("read all data file cost: " + (t2 - t1) + " ms");
    }


    private void readAndDispatch(String filename) throws IOException {
        FileChannel fc = new RandomAccessFile(filename, "r").getChannel();
        int size = (int) fc.size();
        MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, 0, size);

        long t1 = System.currentTimeMillis();

        byte b;
        int length = 0;
        boolean readFlag = true;
        spin = 0;
        while (readFlag) {
            int remaining = size - buffer.position();
            if (remaining >= DataConstants.READ_BUFFER_SIZE) {
                length = DataConstants.READ_BUFFER_SIZE + 4;
                buffer.get(readBuffer, 4, DataConstants.READ_BUFFER_SIZE);
                if (readBuffer[length - 1] != '\n') {
                    while (true) {
                        b = buffer.get();
                        readBuffer[length++] = b;
                        if (b == '\n') {
                            break;
                        }
                    }
                }
                setLength(readBuffer, length - 4);
            } else {
                if(remaining == 0) {
                    break;
                }
                length = remaining + 4;
                buffer.get(readBuffer, 4, remaining);
                setLength(readBuffer, length - 4);

                readFlag = false;
            }

            RingBuffer currentRingBuffer = ringBuffers[ringBufferIndex % DataConstants.PARSER_COUNT];
            while (!currentRingBuffer.put(readBuffer, length)) {
                spin++;
            }
            ringBufferIndex++;
        }

        long t2 = System.currentTimeMillis();
        logger.info(filename + " size: " + fc.size());
        logger.info(filename + " read cost time: " + (t2 - t1) + " ms");
        logger.info(filename + " spin: " + spin);
    }

    public void setLength(byte[] readBuffer, int length) {
        for (int i = 0; i < 4; i++) {
            readBuffer[3 - i] = (byte) (length >> (i << 3) & 0xFF);
        }
    }
}
