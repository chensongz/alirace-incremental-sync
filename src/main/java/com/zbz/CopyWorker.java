package com.zbz;

import java.io.*;

/**
 * Created by Victor on 2017/6/16.
 */
public class CopyWorker {

    private String oldName;
    private String newName;

    CopyWorker(String oldName, String newName) {
        this.oldName = oldName;
        this.newName = newName;
    }

    public void compute() {
        BufferedReader reader;
        BufferedWriter writer;
        try {
             reader = new BufferedReader(new FileReader(oldName));
             writer = new BufferedWriter(new FileWriter(newName));

            String line;
            while((line =  reader.readLine()) != null) {

                int i = 0;
                int cnt = 0;
                int m = 0;
                while(cnt < 5) {
                    i = line.indexOf('|', m);
                    m = i + 1;
                    cnt++;
                }

                line = line.substring(i + 1);
                writer.write(line);
                writer.newLine();
            }

            reader.close();
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        }
    }
}