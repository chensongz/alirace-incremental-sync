package com.zbz.bgk;

import com.zbz.Pool;
import com.zbz.Reducer;

import java.io.IOException;

/**
 * Created by bgk on 6/9/17.
 */
public class ParseTest {
    public static void main(String[] args) throws IOException {
        long t1 = System.currentTimeMillis();
        System.out.println("starat ---");
        Reducer testReducer = new Reducer(600, 700, new Pool<String>(500));
        testReducer.run();
        long t2 = System.currentTimeMillis();
        System.out.println("use time " + (t2-t1));
    }
}
