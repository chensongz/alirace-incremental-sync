package com.zbz.bgk;

import com.zbz.Binlog;
import com.zbz.Pool;
import com.zbz.ReduceUtils;
import com.zbz.Reducer;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by bgk on 6/11/17.
 */
public class Test {


    public static void main(String[] args) throws IOException {

    }

    private static long encode(byte[] dataBuf, int position) {
        long result = 0;
        for (int i = position - 1; i >= 0; i--) {
            result <<= 8;
            result |= (dataBuf[i] & 0xff);
        }
        return result;
    }

    private static void decode(long src) {
        byte b;
        while ((b = (byte)(src & 0xff)) != 0) {
            src >>= 8;
            System.out.println("byte: " + b);
        }
    }

}
