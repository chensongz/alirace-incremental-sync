package com.reborn;

import com.alibaba.middleware.race.sync.Server;

import java.io.BufferedOutputStream;

/**
 * Created by bgk on 6/27/17.
 */
public class Reducer {

    private Server server;
    private int start;
    private int end;

    private volatile int threadIndex = 0;

    public Reducer(Server server, int start, int end) {
        this.server = server;
        this.start = start;
        this.end = end;
    }
}
