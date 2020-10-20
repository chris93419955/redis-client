package com.auto.demo.redis.client;

import com.auto.demo.redis.client.handler.ClientHandlerInitilizer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.Data;

import java.util.concurrent.TimeUnit;

/**
 * @author wbs
 * @date 2020/10/20
 */
@Data
public class NettyClient {

    private EventLoopGroup group = new NioEventLoopGroup();

    private int port;

    private String host;

    private Channel channel;

    public NettyClient(int port, String host) {
        this.port = port;
        this.host = host;
        start();
    }

    public NettyClient() {
        this.port = 6379;
        this.host = "127.0.0.1";
        start();
    }

    public void start() {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .remoteAddress(host, port)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ClientHandlerInitilizer());
        ChannelFuture future = bootstrap.connect();
        //客户端断线重连逻辑
        future.addListener((ChannelFutureListener) future1 -> {
            if (future1.isSuccess()) {
                System.out.println("连接Netty服务端成功");
            } else {
                System.out.println("连接失败，进行断线重连");
                future1.channel().eventLoop().schedule(() -> start(), 20, TimeUnit.SECONDS);
            }
        });

        channel = future.channel();

    }


}
