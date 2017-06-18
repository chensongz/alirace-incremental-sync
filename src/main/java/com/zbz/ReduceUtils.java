package com.zbz;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

/**
 * Created by bgk on 6/18/17.
 */
public class ReduceUtils {
    public static long bytes2Long(byte[] bytes, int len){
        long result = 0;
        for (int i = 0; i < len; i++) {
            result = result * 10 + (bytes[i] - '0');
        }
        return result;
    }
}
