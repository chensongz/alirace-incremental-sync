package com.alibaba.middleware.race.sync;


public class Constants {
    public static final int DATA_FILE_NUM = 3;
    // 工作主目录
    public static final String TESTER_HOME = "/home/zwy/work/middlewareTester";
    // 赛题数据
    public static final String DATA_HOME = "/home/zwy/work/test";
    // 结果文件目录
    public static final String RESULT_HOME = "/home/zwy/work/middlewareTester";
    // teamCode
    public static final String TEAMCODE = "76934cqcw0";
    // 日志级别
    public static final String LOG_LEVEL = "INFO";
    // 中间结果目录
    public static final String MIDDLE_HOME = "/home/zwy/work/middlewareTester/middle";
    // server端口
    public static final Integer SERVER_PORT = 5527;
    // 结果文件的命名
    public static final String RESULT_FILE_NAME = "Result.rs";
    // 拼接数据文件名
    public static String getDataFile(int i) {
        return DATA_HOME + "/canal_0" + i;
    }
}
