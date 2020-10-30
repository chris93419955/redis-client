package com.auto.demo.controller;

import com.auto.demo.redis.client.NettyClient;
import com.auto.demo.redis.core.LettuceFutures;
import com.auto.demo.redis.core.RedisFuture;
import com.auto.demo.redis.core.protocol.RedisCommand;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.TimeUnit;

/**
 * @author wbs
 * @date 2020/10/30
 */
@RestController
public class DemoController {

    @Autowired
    private NettyClient nettyClient;

    @GetMapping("/get")
    public String get(String key) {
        RedisCommand result = nettyClient.get(key);
        if (result instanceof RedisFuture) {
            RedisFuture<String> command = (RedisFuture<String>) result;
            return LettuceFutures.awaitOrCancel(command, 5, TimeUnit.SECONDS);
        }
        return "";
    }

    @GetMapping("/set")
    public String set(String key, String value) {
        RedisCommand result = nettyClient.set(key, value);
        if (result instanceof RedisFuture) {
            RedisFuture<String> command = (RedisFuture<String>) result;
            return LettuceFutures.awaitOrCancel(command, 5, TimeUnit.SECONDS);
        }
        return "";
    }

}
