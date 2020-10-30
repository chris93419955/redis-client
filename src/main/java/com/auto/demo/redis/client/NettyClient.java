package com.auto.demo.redis.client;

import com.auto.demo.redis.client.handler.ClientHandlerInitilizer;
import com.auto.demo.redis.core.RedisCommandBuilder;
import com.auto.demo.redis.core.codec.StringCodec;
import com.auto.demo.redis.core.protocol.AsyncCommand;
import com.auto.demo.redis.core.protocol.RedisCommand;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.TimeUnit;

/**
 * @author wbs
 * @date 2020/10/20
 */
@Slf4j
@Component
public class NettyClient {

    private EventLoopGroup group = new NioEventLoopGroup();

    @Value("${redis.port}")
    private int port;

    @Value("${redis.host}")
    private String host;

    private Channel channel;

    @PostConstruct
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
                log.info("连接Netty服务端成功");
            } else {
                log.info("连接失败，进行断线重连");
                future1.channel().eventLoop().schedule(() -> start(), 20, TimeUnit.SECONDS);
            }
        });

        channel = future.channel();

    }

    @PreDestroy
    public void preDestroy() {
        log.info("shutdown--------------");
        group.shutdownGracefully();
    }

    public RedisCommand get(String key) {
        RedisCommandBuilder commandBuilder = new RedisCommandBuilder<>(StringCodec.UTF8);
        AsyncCommand asyncCommand = new AsyncCommand<>(commandBuilder.get(key));
        channel.writeAndFlush(asyncCommand);
        return asyncCommand;
    }


}
