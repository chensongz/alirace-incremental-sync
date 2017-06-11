package com.zbz;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by zwy on 17-6-11.
 */
public class SendPool {
    private static final SendPool sendPool = new SendPool();
    public static SendPool getInstance() { return sendPool; }

    private BlockingQueue<Record> records = new LinkedBlockingQueue<>(5000);

    public void put(Record record) {
        //synchronize
        try {
            records.put(record);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public Record poll() {
        //synchronize
        try {
            return records.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }


}
