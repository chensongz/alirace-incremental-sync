package com.zbz;

/**
 * Created by bgk on 6/18/17.
 */
public class ReduceUtils {
    public static long bytes2Long(byte[] bytes, int len) {
        long result = 0;
        for (int i = 0; i < len; i++) {
            result = (result << 3) + (result << 1) + (bytes[i] - '0');
        }
        return result;
    }

    public static int bytes2Int(byte[] bytes, int len) {
        int result = 0;
        for (int i = 0; i < len; i++) {
            result = (result << 3) + (result << 1) + (bytes[i] - '0');
        }
        return result;
    }
}
