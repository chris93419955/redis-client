package com.auto.demo.redis.client.handler;

import com.auto.demo.redis.core.protocol.CommandEncoder;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;

/**
 * @author wbs
 * @date 2020/10/20
 */
public class ClientHandlerInitilizer extends ChannelInitializer<Channel> {
    @Override
    protected void initChannel(Channel channel) throws Exception {
        channel.pipeline()
                .addLast(new CommandEncoder())
                .addLast(new RedisClientHandler());
    }
}
