package com.zbz.zwy;

import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by zwy on 17-6-12.
 */
public class TaskQueue {
    private static final TaskQueue tasks = new TaskQueue();

    public synchronized static TaskQueue getInstance() {
        return tasks;
    }
}
