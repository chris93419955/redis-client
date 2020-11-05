package com.auto.demo.redis.example.sync;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author wbs
 * @date 2020/11/3
 */
public class SyncAndAsync {

    public static void main(String[] args) throws Exception {
        System.out.println("main start---------");
        ExecutorService executor = Executors.newFixedThreadPool(5);
        futureDemo(executor);

        completableFutureDemo();

        System.out.println("main end---------");
        Thread.sleep(10000);
//        executor.shutdown();
    }

    private static void futureDemo(ExecutorService executor) throws Exception {
        Response resp = new Response().setId(1000L);
        Future<Response> future = executor.submit(() -> getRemoteResult(resp));
        System.out.println(future.isDone());
        System.out.println(future.get().getContent());
        System.out.println(future.isDone());
    }


    private static void completableFutureDemo() {
        Response resp = new Response().setId(1000L);
        CompletableFuture<Response> future = CompletableFuture.supplyAsync(() -> getRemoteResult(resp));
//        future.thenAccept(result -> System.out.println(result.getContent()));
        future.whenComplete((v, e) -> {
            System.out.println(v.getContent());
            System.out.println(e.getMessage());
        });
    }


    /**
     * 用随机等待模拟响应时间
     *
     * @param resp
     * @return
     */
    private static Response getRemoteResult(Response resp) {
        try {
            long t = (long) (Math.random() * 10000);
            Thread.sleep(t);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return resp.setContent("response content");
    }


}


