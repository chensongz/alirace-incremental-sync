package com.zbz.bgk;

import com.carrotsearch.hppc.LongObjectHashMap;
import com.zbz.Binlog;
import com.zbz.BinlogFactory;
import com.zbz.BinlogReducer;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectArrayMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.*;

/**
 * Created by bgk on 6/11/17.
 */
public class Test {
    public static void main(String[] args) {
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
        Binlog binlog1 = BinlogFactory.createBinlog(line1);
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
//        TLongObjectHashMap<Binlog> tLongObjectHashMap = new TLongObjectHashMap<>();
//        LongObjectHashMap<Binlog> longObjectHashMap = new LongObjectHashMap<>();
//        Long2ObjectOpenHashMap<Binlog> long2ObjectArrayMap = new Long2ObjectOpenHashMap<>();
//        TLongLongHashMap tLongLongHashMap = new TLongLongHashMap();
//        long t1 = System.currentTimeMillis();
//        Map<Long, Binlog> hashMap = new TreeMap<>();
//        for (long i = 0; i < 10000000; i++) {
//            long2ObjectArrayMap.put(i, binlog1);
//        }
//        long t2 = System.currentTimeMillis();
//        System.out.println("use time:" + (t2-t1));
//        for (long i = 0; i < 10000000; i++) {
//            long2ObjectArrayMap.get(i);
//        }
////        hashMap = null;
//        long t3 = System.currentTimeMillis();
//        System.out.println(tLongObjectHashMap.size());
//        System.out.println("use time:" + (t3-t2));




    }
}
