package com.auto.demo.redis.example.proxy;

import com.auto.demo.redis.client.handler.ClientHandlerInitilizer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * @author wbs
 * @date 2020/11/5
 */
public class ClientDemo implements ClientService {

    @Override
    public Object exec(String command) {
        EventLoopGroup group = new NioEventLoopGroup();

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .remoteAddress("47.100.3.29", 9001)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ClientHandlerInitilizer());
        ChannelFuture future = bootstrap.connect();
        //客户端断线重连逻辑
        future.addListener((ChannelFutureListener) future1 -> {
            if (future1.isSuccess()) {
                System.out.println("连接Netty服务端成功");
                future1.channel().writeAndFlush(command);
            } else {
                System.out.println("连接失败");
            }
        });

        return null;
    }
}
