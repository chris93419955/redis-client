package com.auto.demo.redis.client.handler;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.redis.RedisArrayAggregator;
import io.netty.handler.codec.redis.RedisBulkStringAggregator;
import io.netty.handler.codec.redis.RedisDecoder;
import io.netty.handler.codec.redis.RedisEncoder;

/**
 * @author wbs
 * @date 2020/10/20
 */
public class ClientHandlerInitilizer extends ChannelInitializer<Channel> {
    @Override
    protected void initChannel(Channel channel) throws Exception {
        channel.pipeline()
                .addLast(new RedisDecoder())
                .addLast(new RedisBulkStringAggregator())
                .addLast(new RedisArrayAggregator())
                .addLast(new RedisEncoder())
                .addLast(new RedisClientHandler());
    }
}
