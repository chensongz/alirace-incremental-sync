package com.alibaba.middleware.race.sync;

public class Constants {
    public static final int DATA_FILE_NUM = 10;
    // teamCode
    public static final String TEAMCODE = "76934cqcw0";
    // 日志级别
    public static final String LOG_LEVEL = "INFO";
    // server端口
    public static final Integer SERVER_PORT = 5527;

    // ------------ 正式比赛指定的路径--------------//
    //// 工作主目录
    public static final String TESTER_HOME = "/home/admin";
    //// 赛题数据
    public static final String DATA_HOME = "/home/admin/canal_data";
    //// 结果文件目录(client端会用到)
    public static final String RESULT_HOME = "/home/admin/sync_results/" + TEAMCODE;
    //// 中间结果目录（client和server都会用到）
    public static final String MIDDLE_HOME = "/home/admin/middle/" + TEAMCODE;
    // 结果文件的命名
    public static final String RESULT_FILE_NAME = "Result.rs";
    // 拼接数据文件名
    public static String getDataFile(int i) {
        return DATA_HOME + "/" + (1 + i) + ".txt";
    }
}
