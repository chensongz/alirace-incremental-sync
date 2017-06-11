package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.List;

/**
 * Created by wanshao on 2017/5/25.
 */
public class ClientDemoInHandler extends ChannelInboundHandlerAdapter {

    private static Logger logger = LoggerFactory.getLogger(ClientDemoInHandler.class);

    private FileChannel fc;
    private FileChannel fc2;

    // 接收server端的消息，并打印出来
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        logger.info("com.alibaba.middleware.race.sync.ClientDemoInHandler.channelRead");
        ByteBuf result = (ByteBuf) msg;
        byte[] result1 = new byte[result.readableBytes()];
        result.readBytes(result1);


        logger.warn("client receive: " + new String(result1));
        result.release();
        if (result1[result1.length - 1] == '\r') {
            fc.write(ByteBuffer.wrap(result1, 0, result1.length - 1));
            fc2.write(ByteBuffer.wrap(result1, 0, result1.length - 1));
            logger.info("client receive all message success!!");
            logger.warn("result size: " + fc.size());
            fc.close();
            ctx.close();
            //watch
            listDir();
        } else {
            fc.write(ByteBuffer.wrap(result1));
            fc2.write(ByteBuffer.wrap(result1));
        }

        ctx.writeAndFlush("I have received your messages and wait for next messages");
    }

    // 连接成功后，向server发送消息
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        logger.info("com.alibaba.middleware.race.sync.ClientDemoInHandler.channelActive");
        fc = new RandomAccessFile(Constants.RESULT_HOME + "/"
                + Constants.RESULT_FILE_NAME, "rw").getChannel();
        fc2 = new RandomAccessFile("/home/admin/logs/" + Constants.TEAMCODE + "/"
                + Constants.RESULT_FILE_NAME, "rw").getChannel();
        String msg = "I am prepared to receive messages";
        ByteBuf encoded = ctx.alloc().buffer(4 * msg.length());
        encoded.writeBytes(msg.getBytes());
        ctx.write(encoded);
        ctx.flush();
    }

    private void listDir() {
        File dir = new File(Constants.RESULT_HOME);
        List<String> list = Arrays.asList(dir.list());
        logger.info(list.toString());
        System.out.println(list.toString());
    }
}
