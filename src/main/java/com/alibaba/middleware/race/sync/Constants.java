package com.alibaba.middleware.race.sync;

/**
 * 外部赛示例代码需要的常量 Created by zwy on 2017/5/25.
 */
interface Constants {

    // ------------ 本地测试可以使用自己的路径--------------//

    // 工作主目录
    String TESTER_HOME = "/home/zwy/work/middlewareTester";
    // 赛题数据
//    String DATA_HOME = "/home/zwy/work/canal_data";
    String DATA_HOME = "/home/zwy/work/test/canal.txt";
    // 结果文件目录
    String RESULT_HOME = "/home/zwy/work/middlewareTester/user_result";
    // teamCode
    String TEAMCODE = "zwy_test";
    // 日志级别
    String LOG_LEVEL = "INFO";
    // 中间结果目录
    String MIDDLE_HOME = "/home/zwy/work/middlewareTester/middle";
    // server端口
    Integer SERVER_PORT = 5527;

    // ------------ 正式比赛指定的路径--------------//
    //// 工作主目录
//    String TESTER_HOME = "/home/admin";
    //// 赛题数据
    // String DATA_HOME = "/home/admin/canal_data";
    //// 结果文件目录(client端会用到)
    // String RESULT_HOME = "/home/admin/sync_results/${teamcode}";
    //// 中间结果目录（client和server都会用到）
    // String MIDDLE_HOME = "/home/admin/middle/${teamcode}";

    // 结果文件的命名
    // String RESULT_FILE_NAME = "Result.rs";

}
