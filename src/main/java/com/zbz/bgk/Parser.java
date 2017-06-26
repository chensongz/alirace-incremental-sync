package com.zbz.bgk;

/**
 * Created by bgk on 6/17/17.
 */

import com.alibaba.middleware.race.sync.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Parser implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    private RingBuffer<byte[]> ringBuffer;
    public Parser(RingBuffer<byte[]> ringBuffer) {
       this.ringBuffer = ringBuffer;
    }

    @Override
    public void run() {
        while (true) {
            byte[] value;
            while ((value = ringBuffer.get()) == null) {
            }
        }
    }

}

