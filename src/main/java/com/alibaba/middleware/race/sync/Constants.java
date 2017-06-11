package com.alibaba.middleware.race.sync;

/**
 * 外部赛示例代码需要的常量 Created by zwy on 2017/5/25.
 */
public class Constants {

    // ------------ 本地测试可以使用自己的路径--------------//

    // 工作主目录
//    public static final String TESTER_HOME = "/home/zwy/work/middlewareTester";
    // 赛题数据
//    String DATA_HOME = "/home/zwy/work/canal_data";
//    public static final String DATA_HOME = "/home/zwy/work/test/";
    // 结果文件目录
//    public static final String RESULT_HOME = "/home/zwy/work/middlewareTester";
    // teamCode
    public static final String TEAMCODE = "76934cqcw0";
    // 日志级别
    public static final String LOG_LEVEL = "INFO";
    // 中间结果目录
//    public static final String MIDDLE_HOME = "/home/zwy/work/middlewareTester/middle";
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

}
