package com.zbz.zwy;

/**
 * Created by zwy on 17-6-12.
 */
public class BinlogReduceDemo {

    public static void main(String[] args) {
        TaskQueue tasks = TaskQueue.getInstance();

        int cnt = 10;
        while(cnt >= 1) {
            int i;
            for(i = 1; i < cnt; i += 2) {
                Task task = new Task(i, i + 1);
            }
            cnt = cnt / 2 + 1;
        }
    }
}
