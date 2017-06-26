package com.zbz.bgk;

import com.zbz.DataConstants;

import java.io.IOException;

/**
 * Created by bgk on 6/11/17.
 */
public class Test {


    public static void main(String[] args) throws IOException {
        RingBuffer<byte[]>[] ringBuffers = new RingBuffer[DataConstants.PARSER_COUNT];
        Parser[] parsers = new Parser[DataConstants.PARSER_COUNT];
        Thread[] parserThreads = new Thread[DataConstants.PARSER_COUNT];
        for (int i = 0; i < DataConstants.PARSER_COUNT; i++) {
            byte[][] buff = new byte[DataConstants.RINGBUFFER_CAPACITY][];
            ringBuffers[i] = new RingBuffer<>(buff);
            parsers[i] = new Parser(ringBuffers[i]);
            parserThreads[i] = new Thread(parsers[i]);
            parserThreads[i].start();
        }
        Reader reader = new Reader(ringBuffers);
        reader.run();


    }

}
