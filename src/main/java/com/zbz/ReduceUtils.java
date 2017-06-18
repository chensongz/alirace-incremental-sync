package com.zbz;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

/**
 * Created by bgk on 6/18/17.
 */
public class ReduceUtils {
    public static void skipLineUseless(ByteBuffer byteBuffer) {
        // skip |mysql-bin.000017630680234|1496737946000|middleware3|student|
        skip(byteBuffer, DataConstans.BINARY_PRE_SIZE);
        skipUntilCharacter(byteBuffer, DataConstans.SEPARATOR);
        skip(byteBuffer, DataConstans.OTHER_PRE_SIZE);
    }

    public static void skip(ByteBuffer byteBuffer, int skipCount) {
        byteBuffer.position(byteBuffer.position() + skipCount);
    }

    public static void skipUntilCharacter(ByteBuffer byteBuffer, byte skipCharacter) {
        while (byteBuffer.hasRemaining() && byteBuffer.get() != skipCharacter) {
        }
    }

    public static boolean readUntilCharacter(ByteBuffer byteBuffer, ByteArrayOutputStream bao, byte skipCharacter) {
        bao.reset();
        byte b;
        while (byteBuffer.hasRemaining()) {
            b = byteBuffer.get();
            if (b == DataConstans.LF) return false;
            if (b != skipCharacter) {
                bao.write(b);
            } else {
                break;
            }
        }
        return true;
    }


    public  void skipLineUseless(ByteBuffer byteBuffer) {
        // skip |mysql-bin.000017630680234|1496737946000|middleware3|student|
        skip(byteBuffer, DataConstans.BINARY_PRE_SIZE);
        skipUntilCharacter(byteBuffer, DataConstans.SEPARATOR);
        skip(byteBuffer, DataConstans.OTHER_PRE_SIZE);
    }

    public  void skip(ByteBuffer byteBuffer, int skipCount) {
        byteBuffer.position(byteBuffer.position() + skipCount);
    }

    public  void skipUntilCharacter(ByteBuffer byteBuffer, byte skipCharacter) {
        while (byteBuffer.hasRemaining() && byteBuffer.get() != skipCharacter) {
        }
    }

    public  boolean readUntilCharacter(ByteBuffer byteBuffer, byte[] bao, byte skipCharacter) {
        reset();
        byte b;
        while (byteBuffer.hasRemaining()) {
            b = byteBuffer.get();
            if (b == DataConstans.LF) return false;
            if (b != skipCharacter) {
                write(b);
            } else {
                break;
            }
        }
        return true;
    }
    public static long bytes2Long(byte[] bytes, int len){
        long result = 0;
        for (int i = 0; i < len; i++) {
            result = result * 10 + (bytes[i] - '0');
        }
        return result;
    }

}
