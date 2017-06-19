package com.alibaba.middleware.race.sync;

import com.zbz.Pool;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 处理client端的请求 Created by wanshao on 2017/5/25.
 */
public class ServerDemoInHandler extends ChannelInboundHandlerAdapter {

    private static Logger logger = LoggerFactory.getLogger(Server.class);

    private Pool<String> sendPool;

    public ServerDemoInHandler() {
        try {
            sendPool = Pool.getPoolInstance(String.class, 8192);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据channel
     *
     * @param ctx
     * @return
     */
    public static String getIPString(ChannelHandlerContext ctx) {
        String ipString = "";
        String socketString = ctx.channel().remoteAddress().toString();
        int colonAt = socketString.indexOf(":");
        ipString = socketString.substring(1, colonAt);
        return ipString;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        String ipString = getIPString(ctx);
        // 保存channel
        Server.getMap().put(ipString, ctx.channel());

        logger.info("com.alibaba.middleware.race.sync.ServerDemoInHandler.channelRead");
        ByteBuf result = (ByteBuf) msg;
        byte[] result1 = new byte[result.readableBytes()];
        // msg中存储的是ByteBuf类型的数据，把数据读取到byte[]中
        result.readBytes(result1);
        String resultStr = new String(result1);
        // 接收并打印客户端的信息
        logger.info("receive from client:" + resultStr);
        Channel channel = Server.getMap().get(ipString);

        long t1 = System.currentTimeMillis();
        while (true) {
            // 向客户端发送消息
            String message = (String) getMessage();
//            logger.info("send message: " + message);
            if (message != null) {
                ByteBuf byteBuf = Unpooled.wrappedBuffer((message).getBytes());
                channel.writeAndFlush(byteBuf).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
//                        logger.info("Server send a message.");
                    }
                });
            } else {
                ByteBuf byteBuf = Unpooled.wrappedBuffer(new byte[]{'\r'});
                channel.writeAndFlush(byteBuf).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        logger.info("Server send all message success!!");
                    }
                });
                break;
            }
        }
        long t2 = System.currentTimeMillis();
        logger.info("Server send cost: " + (t2 - t1) + "ms");
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    private Object getMessage() throws InterruptedException {
        String message = sendPool.poll();
        if (message.equals("NULL")) {
            return null;
        } else {
            return message;
        }
    }
}
