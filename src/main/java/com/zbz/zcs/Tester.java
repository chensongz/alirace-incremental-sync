package com.zbz.zcs;

import com.alibaba.middleware.race.sync.Constants;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by zhuchensong on 6/9/17.
 */
public class Tester {
    public static void main(String[] args) {
        try {
            FileChannel fc = new RandomAccessFile(Constants.MIDDLE_HOME + "/test-file", "rw").getChannel();
            String msg = "This is a test msg";
            fc.write(ByteBuffer.wrap(msg.getBytes()));
            ByteBuffer byteBuffer = ByteBuffer.allocate(100);
            fc.read(byteBuffer, 0);

            System.out.println("before flip:");
            printByteBufferInfo(byteBuffer);

            byteBuffer.flip();

            System.out.println("after flip:");
            printByteBufferInfo(byteBuffer);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void printByteBufferInfo(ByteBuffer byteBuffer) {
        System.out.println("byteBuffer position: " + byteBuffer.position());
        System.out.println("byteBuffer limit: " + byteBuffer.limit());
        System.out.println("read msg: " + new String(byteBuffer.array()));
        System.out.println("read msg length: " + byteBuffer.array().length);
        System.out.println("read msg string length: " + new String(byteBuffer.array()).getBytes().length);
    }
}
