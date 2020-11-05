package com.auto.demo.redis.example.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

/**
 * @author wbs
 * @date 2020/11/5
 */
public class ClientHandler implements InvocationHandler {

    Object target;

    public ClientHandler(ClientService client) {
        this.target = client;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        System.out.println("proxy ----------start-------");
        Object result = method.invoke(target, args);
        System.out.println("result -----------" + result.toString());
        System.out.println("proxy -----------end----------");
        return null;
    }
}
