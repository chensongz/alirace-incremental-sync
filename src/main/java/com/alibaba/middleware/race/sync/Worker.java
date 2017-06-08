package com.alibaba.middleware.race.sync;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * Created by zwy on 17-6-8.
 */
public class Worker implements Runnable {

    public void run() {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(Constants.DATA_HOME));
            String line;
            int i = 0;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                if (i++ >= 500) break;
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
