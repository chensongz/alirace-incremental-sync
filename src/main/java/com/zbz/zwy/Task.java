package com.zbz.zwy;

/**
 * Created by zwy on 17-6-12.
 */
public class Task {
    private String file1;
    private String file2;

    public Task(int file1, int file2) {
        this(file1);
        this.file2 = String.valueOf(file2) + ".test";
    }

    public Task(int file1) {
        this();
        this.file1 = String.valueOf(file1) + ".test";
    }

    public Task() {
        this.file1 = null;
        this.file2 = null;
    }

    public String getFile1() {
        return file1;
    }

    public String getFile2() {
        return file2;
    }
}
