package com.auto.demo.redis;

import com.auto.demo.redis.client.NettyClient;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.GenericFutureListener;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * @author wbs
 * @date 2020/10/20
 */
public class Application {

    public static void main(String[] args) throws Exception {
        NettyClient client = new NettyClient();
        try {
            Channel ch = client.getChannel();

            // 读取控制台命令
            System.out.println("Enter Redis commands (quit to end)");
            ChannelFuture lastWriteFuture = null;
            BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
            while (true) {
                final String input = in.readLine();
                final String command = input != null ? input.trim() : null;
                //quit exit 命令来退出
                if (command == null || "quit".equalsIgnoreCase(command) || "exit".equalsIgnoreCase(command)) {
                    ch.close().sync();
                    break;
                } else if (command.isEmpty()) {
                    continue;
                }
                // 发送命令到redis
                lastWriteFuture = ch.writeAndFlush(command);
                lastWriteFuture.addListener(new GenericFutureListener<ChannelFuture>() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if (!future.isSuccess()) {
                            System.err.print("write failed: ");
                            future.cause().printStackTrace(System.err);
                        }
                    }
                });
            }

            // 释放资源
            if (lastWriteFuture != null) {
                lastWriteFuture.sync();
            }
        } finally {
            client.getGroup().shutdownGracefully();
        }
    }


}
