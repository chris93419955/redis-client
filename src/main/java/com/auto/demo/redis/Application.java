package com.auto.demo.redis;

import com.auto.demo.redis.client.NettyClient;
import com.auto.demo.redis.core.LettuceFutures;
import com.auto.demo.redis.core.RedisCommandBuilder;
import com.auto.demo.redis.core.RedisFuture;
import com.auto.demo.redis.core.codec.StringCodec;
import com.auto.demo.redis.core.protocol.AsyncCommand;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.GenericFutureListener;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

/**
 * @author wbs
 * @date 2020/10/20
 */
public class Application {


    public static void main(String[] args) throws Exception {
        NettyClient client = new NettyClient();
        try {
            Channel ch = client.getChannel();

            RedisCommandBuilder commandBuilder = new RedisCommandBuilder<>(StringCodec.UTF8);

            AsyncCommand asyncCommand = new AsyncCommand<>(commandBuilder.get("k"));

            ch.writeAndFlush(asyncCommand);
            Object result = null;
            if (asyncCommand instanceof RedisFuture) {
                RedisFuture<?> command = (RedisFuture<?>) asyncCommand;
                result = LettuceFutures.awaitOrCancel(command, 5, TimeUnit.SECONDS);
            }


            System.out.println(result.toString());
//            ch.close().sync();
        } finally {
            client.getGroup().shutdownGracefully();
        }
    }




}
