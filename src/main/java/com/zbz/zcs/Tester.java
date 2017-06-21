package com.zbz.zcs;

import com.alibaba.middleware.race.sync.Constants;
import com.zbz.DataConstans;
import com.zbz.ReduceUtils;
import gnu.trove.map.hash.TIntByteHashMap;
import gnu.trove.map.hash.TLongIntHashMap;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by zhuchensong on 6/21/17.
 */
public class Tester {

    private class T{
        private boolean a;

        T() {
            a = true;
        }

        public boolean getA() {
            return a;
        }
    }

    public static void main(String[] args) {
//        testHashMap();
//        testBytes2Long();
//        testBufferGet();
//        testHashAndArray();
        Tester test = new Tester();
        test.testMember();
    }

    private void testMember() {
        T t = new T();

        long t1, t2, t3;
        int count = 100000000;
        t1 = System.currentTimeMillis();
        boolean a = true;
        for(int i = 0; i < count; i++) {
            if(t.getA()){}
        }
        t2 = System.currentTimeMillis();
        for(int i = 0; i < count; i++) {
            if(a){}
        }
        t3 = System.currentTimeMillis();
        System.out.println("heap: " + (t2 - t1));
        System.out.println("stack: " + (t3 - t2));
    }

    private static void testHashAndArray() {
        TIntByteHashMap t = new TIntByteHashMap();
        t.put(4, (byte)1);
        byte[] a = new byte[5];
        a[4] = (byte)1;
        long t1, t2, t3;
        int count = 100000000;
        byte b;
        t1 = System.currentTimeMillis();
        for(int i = 0; i < count; i++) {
            b = t.get(4);
        }
        t2 = System.currentTimeMillis();
        for(int i = 0; i < count; i++) {
            b = a[4];
        }
        t3 = System.currentTimeMillis();
        System.out.println("HashMap: " + (t2 - t1));
        System.out.println("Array: " + (t3 - t2));
    }

    private static void testBufferGet() {
        String filename;
        long t1 = System.currentTimeMillis();
        byte a = (byte) '3';
        for (int i = 0; i < Constants.DATA_FILE_NUM; i++) {
            filename = Constants.getDataFile(i);
            System.out.println("Read " + filename + " start!");
            try {
                FileChannel fc = new RandomAccessFile(filename, "r").getChannel();
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
                long t3 = System.currentTimeMillis();
                int size = (int) fc.size();
                System.out.println("limit: " + buffer.limit());
                System.out.println("capacity: " + buffer.capacity());
                System.out.println("size:" + size);

                while (buffer.position() < size) {
//                while (buffer.hasRemaining()) {
//                    buffer.hasRemaining();
                    if (buffer.get() == 'a') {

                    }
//                    buffer.get();
//                    if (buffer.position() <= size - 1) {
//                        buffer.position(buffer.position() + 1);
//                    try {
//                        buffer.position(buffer.position() + 30);
////                    }
//                    } catch (Exception e) {
////                        e.printStackTrace();
//                    }
                }
                long t4 = System.currentTimeMillis();
                System.out.println(filename + " size: " + fc.size());
                System.out.println(filename + " get byte cost time: " + (t4 - t3) + " ms");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        long t2 = System.currentTimeMillis();
        System.out.println("read all data file cost: " + (t2 - t1) + " ms");
    }

    private static void testHashMap() {

        TLongIntHashMap hashMap = new TLongIntHashMap(DataConstans.HASHMAP_CAPACITY);

        long t1 = System.currentTimeMillis();
        for (int i = 0; i < DataConstans.HASHMAP_CAPACITY; i++) {
            hashMap.put((long) (i + 123456789), i);
        }
        long t2 = System.currentTimeMillis();
        System.out.println("hashmap put cost: " + (t2 - t1) + " ms");

        t1 = System.currentTimeMillis();
        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < DataConstans.HASHMAP_CAPACITY; i++) {
                hashMap.get((long) (i + 123456789));
            }
        }
        t2 = System.currentTimeMillis();
        System.out.println("hashmap get cost: " + (t2 - t1) + " ms");

        t1 = System.currentTimeMillis();
        for (int i = 0; i < 6000000; i++) {
            hashMap.remove((long) (i + 123456789));
        }
        t2 = System.currentTimeMillis();
        System.out.println("hashmap remove cost: " + (t2 - t1) + " ms");

    }

    private static void testBytes2Long() {
        byte[] bytes = new byte[]{'1', '2', '3', '4', '5', '6', '7', '8', '9'};

        int count = 100000000;

        long t1 = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            ReduceUtils.bytes2Long(bytes, 9);
        }
        long t2 = System.currentTimeMillis();
        System.out.println("bytes2Long cost: " + (t2 - t1) + " ms");

        long num = 123456789;

        t1 = System.currentTimeMillis();
        for (int i = 0; i < 1500000; i++) {
            long2Bytes(num);
        }
        t2 = System.currentTimeMillis();
        System.out.println("long2Bytes cost: " + (t2 - t1) + " ms");

    }

    private static byte[] long2Bytes(long src) {
        byte b;
        long x = src;
        long p;
        int len = 0;
        while (x > 0) {
            x /= 10;
            len++;
        }
        byte[] bytes = new byte[len];
        for (int i = len - 1; i >= 0; i--) {
            p = power(i);
            b = (byte) (src / p);
            src -= b * p;
            b += (byte) '0';
            bytes[i] = b;
        }
        return bytes;
    }

    private static long power(int num) {
        long ret = 1;
        while (num-- > 0) {
            ret *= 10;
        }
        return ret;
    }

}
