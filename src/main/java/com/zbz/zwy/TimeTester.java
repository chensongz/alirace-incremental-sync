package com.zbz.zwy;

/**
 * Created by zwy on 17-6-12.
 */
public class TimeTester {

    private static TimeTester instance = new TimeTester();

    public static TimeTester getInstance() { return instance; }

    private long t1;
    private long t2;

    public synchronized void setT1(long t) { t1 = t; }
    public synchronized void setT2(long t) { t2 = t; }
    public synchronized long getT1() { return t1; }
    public synchronized long getT2() { return t2; }
}
