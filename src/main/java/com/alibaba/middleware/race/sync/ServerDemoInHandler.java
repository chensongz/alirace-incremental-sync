package com.alibaba.middleware.race.sync;

import com.zbz.Record;
import com.zbz.SendPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * 处理client端的请求 Created by wanshao on 2017/5/25.
 */
public class ServerDemoInHandler extends ChannelInboundHandlerAdapter {

    private static Logger logger = LoggerFactory.getLogger(ServerDemoInHandler.class);

    private SendPool sendPool;

    public ServerDemoInHandler() {
        sendPool = SendPool.getInstance();
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
        logger.info("receive client:" + resultStr);
        Channel channel = Server.getMap().get(ipString);

        long t1 = System.currentTimeMillis();
        while (true) {
            // 向客户端发送消息
            String message = (String) getMessage();
            if (message != null) {
                ByteBuf byteBuf = Unpooled.wrappedBuffer((message + "\n").getBytes());
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
        String p = "Server sender: " + (t2 - t1) + "ms";
        System.out.println(p);
        logger.info(p);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    private Object getMessage() throws InterruptedException {
        // 模拟下数据生成，每隔5秒产生一条消息
//        Thread.sleep(5000);
        Record record = sendPool.poll();
        logger.warn("server polled record: " + record.toString());
        if (record.getTable() != null) {
            return record.toString();
        } else {
            return null;
        }
//        return "message generated in ServerDemoInHandler";

    }
}
