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
//        System.out.println("put record: " + record);
        try {
            records.put(record);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public Record poll() {
        //synchronize
        try {
            Record record = records.take();
//            System.out.println("poll record: " + record);
            return record;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }


}
