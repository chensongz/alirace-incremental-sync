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

    public static long f0(long a) {
        return a * 10;
    }

    public static long f1(long a) {
        return (a << 3) + (a << 1);
    }

    public static void main(String[] args) throws IOException {
        System.out.println(f0(55));
        System.out.println(f1(55));

        int n = 5240296;

        Reducer r = new Reducer(0,0);

        byte[] bytes = {'1', '2', '3', '4', '5', '6', '7'};
        OutputStream out = new ByteArrayOutputStream();

        long t1, t2, t3, tt1 = 0, tt2 = 0;
        TLongLongHashMap map = new TLongLongHashMap();
        t1 = System.currentTimeMillis();
        for(int i = 0; i < 5240296; i++) {
            map.put(i, i);
        }
        t2 = System.currentTimeMillis();
        tt1 += (t2 - t1);
        t1 = System.currentTimeMillis();
        for(int i = 0; i < 1979570; i++) {
            map.remove(i);
        }
        t2 = System.currentTimeMillis();
        tt2 += (t2 - t1);

        System.out.println("tt1: " + tt1);
        System.out.println("tt2: " + tt2);

//        String str = "a|bnc|23|NULL|";
//        String[] strings = str.split("\\|");
//        System.out.println(strings.length);
//        System.out.println(strings[0]);
//        for (int i= 0; i < strings.length; i++) {
//            System.out.println(strings[i]);
//        }

//        LinkedHashMap<String, String> linkedHashMap = new LinkedHashMap<>();
//        linkedHashMap.put("first_name", "haha");
//
//        linkedHashMap.put("first_name", "www");
//
//        for (String value : linkedHashMap.values()) {
//            System.out.println(value);
//        }

//        List<String> list = Arrays.asList("1","2","3");
//        System.out.println(list);
//        StringBuilder sb = new StringBuilder();
//        for (String value : new String[]{"a","b", "c"}) {
//            sb.append(value).append("k");
//        }
//        sb.setLength(sb.length() - 1);
//        System.out.println(sb.toString());
//
//
//        HashMap<Long, Long> index1 = new HashMap<>();
//        HashMap<Long, Long> index2 = new HashMap<>();
//        index1.put(100L, 200L);
//        index1.put(101L, 200L);
//        index1.put(104L, 200L);
//        index2.put(100L, 200L);
//        index2.put(102L, 200L);
//        index2.put(103L, 200L);
//        Set<Long> keys = index1.keySet();
//        keys.retainAll(index2.keySet());
//        System.out.println(keys);
//

//        String line1 = "|mysql-bin.000017659728416|1496746079000|middleware3|student|D|id:1:1|1999996|NULL|first_name:2:0|高|张|";
//        String line2 = "|mysql-bin.000017659728416|1496746079000|middleware3|student|I|id:1:1|NULL|1999996|first_name:2:0|张|高|score:1:0|90|80";
//        String line1 = "|mysql-bin.000017659728416|1496746079000|middleware3|student|U|id:1:1|998|999|first_name:2:0|高|张|";
//        String line2 = "|mysql-bin.000017659728416|1496746079000|middleware3|student|U|id:1:1|999|1999996|first_name:2:0|张|高|score:1:0|90|80";
        String line1 = "|mysql-bin.000017659728416|1496746079000|middleware3|student|I|id:1:1|NULL|999|first_name:2:0|高|张|";
//        String line2 = "|mysql-bin.000017659728416|1496746079000|middleware3|student|D|id:1:1|999|NULL|first_name:2:0|张|高|score:1:0|90|80";

//        String line1 = "|mysql-bin.000017659728416|1496746079000|middleware3|student|I|id:1:1|NULL|1999996|first_name:2:0|张|高|score:1:0|90|80";
//        String line2 = "|mysql-bin.000017659728416|1496746079000|middleware3|student|U|id:1:1|1999996|9997|first_name:2:0|高|张|";
//        Binlog binlog1 = BinlogFactory.createBinlog(line1);
//        Binlog binlog2 = BinlogFactory.createBinlog(line2);
//        System.out.println(binlog1);
//        System.out.println("binlog1 primary key:" + binlog1.getPrimaryKey());
//        System.out.println("binlog1 primary vaule:" + binlog1.getPrimaryValue());
//        System.out.println("binlog1 primary old vaule:" + binlog1.getPrimaryOldValue());
//
//        System.out.println(binlog2);
//        System.out.println("binlog2 primary key:" + binlog2.getPrimaryKey());
//        System.out.println("binlog2 primary vaule:" + binlog2.getPrimaryValue());
//        System.out.println("binlog2 primary old vaule:" + binlog2.getPrimaryOldValue());
//
//        Binlog binlog = BinlogReducer.updateOldBinlog(binlog1, binlog2);
//        System.out.println(binlog);
//        System.out.println("binlog primary key:" + binlog.getPrimaryKey());
//        System.out.println("binlog primary vaule:" + binlog.getPrimaryValue());
//        System.out.println("binlog primary old vaule:" + binlog.getPrimaryOldValue());

//        BinlogReducer binlogReducer = new BinlogReducer("middleware3", "student");
//        binlogReducer.reduce(line1);
//        binlogReducer.reduce(line2);
//
//        for (Binlog binlog : binlogReducer.getBinlogHashMap().values()) {
//            System.out.println(binlog);
//        }
//        HashLongObjMap<Binlog> hashLongObjMap1 = HashLongObjMaps.getDefaultFactory().newMutableMap(500);
//        TLongObjectHashMap<Binlog> hashLongObjMap = new TLongObjectHashMap<>(8388608);
////        LongObjectHashMap<Binlog> longObjectHashMap = new LongObjectHashMap<>();
////        Long2ObjectOpenHashMap<Binlog> long2ObjectArrayMap = new Long2ObjectOpenHashMap<>();
////        TLongLongHashMap tLongLongHashMap = new TLongLongHashMap();
//        long t1 = System.currentTimeMillis();
//        Binlog binlog = new Binlog(1);
////////        Map<Long, Binlog> hashMap = new TreeMap<>();
//        for (long i = 0; i < 10000000; i++) {
//            hashLongObjMap.put(i, binlog);
//        }
//        long t2 = System.currentTimeMillis();
//        System.out.println("use time:" + (t2-t1));
//        for (long i = 0; i < 10000000; i++) {
//            hashLongObjMap.get(i);
//        }
////////        hashMap = null;
//        long t3 = System.currentTimeMillis();
//        System.out.println("use time:" + (t3-t2));
//        for ( int i = 0; i < tLongObjectHashMap.values().length; i++) {
//            Binlog binlog = (Binlog)tLongObjectHashMap.values()[i];
//        }
////        for (long key:tLongObjectHashMap.keys()) {
////            Binlog binlog = tLongObjectHashMap.get(key);
////        }
//        t1 = System.currentTimeMillis();
//        System.out.println("use time 1:" + (t1-t3));

//        long t1,t2;
//        t1 = System.currentTimeMillis();

//        try {
//            try (BufferedReader bufferedReader = new BufferedReader(new FileReader("/home/zwy/work/test/canal.txt"))) {
//                String line;
//                while ((line=bufferedReader.readLine()) !=  null) {
//                    BinlogFactory.createBinlog(line);
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        t2 = System.currentTimeMillis();
//        System.out.println("use time :" + (t2-t1));
//        TLongLongHashMap tLongLongHashMap = new TLongLongHashMap();
//        System.out.println(tLongLongHashMap.get(1));
//        byte[] dataBuf = new byte[]{1,2,3,4,5,6,7,8};
////        System.out.println(encode(dataBuf, 8));
//        decode(encode(dataBuf, 8));
////        System.out.println(((3 & 0xff) << 8));
//        byte[] a = String.valueOf(8889).getBytes();
//        for (int i = 0; i < a.length; i++) {
//            System.out.println(a[i]);
//        }
//        long t1 = System.currentTimeMillis();
//        for (long i = 1000000; i < 8000000; i++) {
////            ReduceUtils.long2Bytes(i);
//        }
//        long t2 = System.currentTimeMillis();
//        System.out.println("use time :" + (t2-t1));
//
//        for (long i = 1000000; i < 8000000; i++) {
//            String.valueOf(i).getBytes();
//        }
//
//        t1 = System.currentTimeMillis();
//
//
//        System.out.println("use time 2 :" + (t1-t2));
//        System.out.println(encode(new byte[]{-1,2,3,4,5,6}, 6));
//        decode(encode(new byte[]{-127,-27,-38,-49,-50,-128}, 6));
//        System.out.println();
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
