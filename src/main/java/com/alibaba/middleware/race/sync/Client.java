package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by wanshao on 2017/5/25.
 */
public class Client {

    private final static int port = Constants.SERVER_PORT;
    private final static int BUF_CAPACITY = 1024 * 64;
    private static String ip;

    public static void main(String[] args) {
        initProperties();
        Logger logger = LoggerFactory.getLogger(Client.class);
        logger.info("Welcome to Client!!");
        ip = args[0];

        Client client = new Client();
        client.startClientSocket(ip, port);
    }

    /**
     * 初始化系统属性
     */
    private static void initProperties() {
        System.setProperty("middleware.test.home", Constants.TESTER_HOME);
        System.setProperty("middleware.teamcode", Constants.TEAMCODE);
        System.setProperty("app.logging.level", Constants.LOG_LEVEL);
    }

    /**
     * 连接服务端
     *
     * @param host
     * @param port
     * @throws Exception
     */
    public void connect(String host, int port) throws Exception {
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(new IdleStateHandler(150, 0, 0));
                    ch.pipeline().addLast(new ClientIdleEventHandler());
                    ch.pipeline().addLast(new ClientDemoInHandler());
                }
            });
            // Start the client.
            ChannelFuture f = b.connect(host, port).sync();
            // Wait until the connection is closed.
            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
        }
    }

    public void startClientSocket(String host, int port) {
        Socket clientSocket = null;
        InetSocketAddress addr = new InetSocketAddress(host, port);
        boolean retry = true;

        while(retry) {
            try {
                clientSocket = new Socket();
                clientSocket.setKeepAlive(true);
                clientSocket.connect(addr);

                retry = false;
            } catch (IOException e) {
//                e.printStackTrace();
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }

        byte[] buf = new byte[BUF_CAPACITY];
        int n;
        try {
            InputStream sockStream = clientSocket.getInputStream();
            FileChannel fc = new RandomAccessFile(Constants.RESULT_HOME + "/"
                    + Constants.RESULT_FILE_NAME, "rw").getChannel();

            while(true) {
                System.out.println("receiving...");
                n = sockStream.read(buf);
                System.out.println("Client received: " + n);
                if(buf[n - 1] == '\r') {
                    System.out.println("Client receive end");
                    fc.write(ByteBuffer.wrap(buf, 0, n - 1));
                    fc.close();
                    sockStream.close();
                    clientSocket.close();
                    break;
                } else {
                    fc.write(ByteBuffer.wrap(buf, 0, n));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
