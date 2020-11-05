package com.auto.demo.redis.example.proxy;

import java.lang.reflect.Proxy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author wbs
 * @date 2020/11/5
 */
public class ProxyDemo {

    public static void main(String[] args) throws Exception {
        System.out.println("main start---------");
        ExecutorService executor = Executors.newFixedThreadPool(5);

//        ClientDemo client = new ClientDemo();
//        executor.submit(() -> client.exec("ping"));
        ClientService proxy = (ClientService) Proxy.newProxyInstance(ClientService.class.getClassLoader(), new Class<?>[]{ClientService.class}, new ClientHandler(new ClientDemo()));
        executor.submit(() -> proxy.exec("ping"));

        System.out.println("main end---------");

    }

}
