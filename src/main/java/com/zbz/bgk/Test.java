package com.zbz.bgk;

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

        List<String> list = Arrays.asList("1","2","3");
        System.out.println(list);
        StringBuilder sb = new StringBuilder();
        for (String value : new String[]{"a","b", "c"}) {
            sb.append(value).append("k");
        }
        sb.setLength(sb.length() - 1);
        System.out.println(sb.toString());
    }
}
