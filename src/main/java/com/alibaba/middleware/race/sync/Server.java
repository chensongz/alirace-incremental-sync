package com.alibaba.middleware.race.sync;

import com.zbz.Reducer;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 服务器类，负责push消息到client Created by wanshao on 2017/5/25.
 */
public class Server {

    private static Logger logger = LoggerFactory.getLogger(Server.class);
    // 保存channel
    private static Map<String, Channel> map = new ConcurrentHashMap<String, Channel>();
    // 接收评测程序的三个参数
    private static String schema;
    private static Map tableNamePkMap;

    public static Map<String, Channel> getMap() {
        return map;
    }

    public static void setMap(Map<String, Channel> map) {
        Server.map = map;
    }

    public static void main(String[] args) throws InterruptedException {

        String schema = args[0];
        String table = args[1];
        long start = Long.parseLong(args[2]);
        long end = Long.parseLong(args[3]);

        initProperties();
        printInput(args);
        Logger logger = LoggerFactory.getLogger(Server.class);
        Server server = new Server();
        logger.info("com.alibaba.middleware.race.sync.Server is running....");


        Reducer reducer = new Reducer((int) start, (int) end);
        reducer.run();


        OutputStream clientStream = server.startServerSocket(Constants.SERVER_PORT);
        BufferedOutputStream bufferedClientStream = new BufferedOutputStream(clientStream, 8192);
        try {
            reducer.sendToSocketDirectly(bufferedClientStream);
            bufferedClientStream.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 打印赛题输入 赛题输入格式： schemaName tableName startPkId endPkId，例如输入： middleware student 100 200
     * 上面表示，查询的schema为middleware，查询的表为student,主键的查询范围是(100,200)，注意是开区间 对应DB的SQL为： select * from middleware.student where
     * id>100 and id<200
     */
    private static void printInput(String[] args) {
        // 第一个参数是Schema Name
        System.out.println("Schema:" + args[0]);
        // 第二个参数是Schema Name
        System.out.println("table:" + args[1]);
        // 第三个参数是start pk Id
        System.out.println("start:" + args[2]);
        // 第四个参数是end pk Id
        System.out.println("end:" + args[3]);
    }

    /**
     * 初始化系统属性
     */
    private static void initProperties() {
        System.setProperty("middleware.test.home", Constants.TESTER_HOME);
        System.setProperty("middleware.teamcode", Constants.TEAMCODE);
        System.setProperty("app.logging.level", Constants.LOG_LEVEL);
    }

    private OutputStream startServerSocket(int port) {

        ServerSocket serverSocket;
        OutputStream clientStream;
        Socket clientSocket;
        try {
            serverSocket = new ServerSocket(port);
            clientSocket = serverSocket.accept();
            InetAddress clientAddr = clientSocket.getInetAddress();

            logger.info("Client" + clientAddr + " connected...");
            clientStream = clientSocket.getOutputStream();
            return clientStream;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private void startServer(int port) throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {

                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            // 注册handler
                            ch.pipeline().addLast(new ServerDemoInHandler());
                            // ch.pipeline().addLast(new ServerDemoOutHandler());
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);
            ChannelFuture f = b.bind(port).sync();
            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }
}
