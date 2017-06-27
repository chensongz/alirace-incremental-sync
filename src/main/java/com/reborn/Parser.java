package com.reborn;

/**
 * Created by bgk on 6/27/17.
 */
public class Parser implements Runnable{

    private int threadNumber;
    private RingBuffer ringBuffer;
    private Reducer reducer;

    public Parser(int threadNumber, RingBuffer ringBuffer, Reducer reducer) {
        this.threadNumber = threadNumber;
        this.ringBuffer = ringBuffer;
        this.reducer = reducer;
    }

    @Override
    public void run() {
        
    }
}
