package com.zbz.bgk;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

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
//        long t1 = System.currentTimeMillis();
//        for (int i = 0; i < 10000000; i++) {
//            String[] strings = "a|bnc|23|NULL|a|bnc|23|NULL|a|bnc|23|NULL|".split("\\|");
//        }
//        long t2 = System.currentTimeMillis();
//        System.out.println("1 use time:" + (t2 - t1));
//        t1 = System.currentTimeMillis();
//        String a = "a|bnc|23|NULL|a|bnc|23|NULL|a|bnc|23|NULL|";
//        for (int i = 0; i < 100000000; i++) {
//            int m = 0;
//           while(true) {
//               int index = a.indexOf("|");
//               if (index < 0 ) break;
//              String str = a.substring(0, index);
//              if (str.equals("23")){
//                  m++;
//              }
//              a = a.substring(index + 1);
//           }
//
//        }
//        t2 = System.currentTimeMillis();
//        System.out.println("2 use time:" + (t2 - t1));int cnt = 0;
        int i = 0;
        int cnt = 0;
        int m = 0;
        String line = "|fads|fasdf|fasdf|fsdf|op";
        while(cnt < 5) {
            i = line.indexOf('|', m);
            m = i + 1;
            cnt++;
        }
        line = line.substring(i + 1);
        System.out.println(line);
    }
}
